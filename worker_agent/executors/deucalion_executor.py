from __future__ import annotations

import logging
import os
import posixpath
import shlex
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

import yaml

from worker_agent.deucalion.config import DeucalionJobConfig, resolve_deucalion_job_config
from worker_agent.deucalion.slurm import ACTIVE_STATES, SlurmState, query_state, sbatch_submit, scancel_job
from worker_agent.deucalion.ssh_client import SSHClient, SSHCommandError, SSHSettings

from .base import BaseExecutor, WorkerRuntime

_LOGGER = logging.getLogger(__name__)
_ARTIFACT_SYNC_RETRY_MAX_BACKOFF_SECONDS = 5.0
_BACKEND_STOP_STATES = {"stop_requested", "canceled", "queued", "failed", "finished", "stopped"}
_BACKEND_OVERRIDE_NO_POST = {"queued", "failed", "finished", "stopped"}


class DeucalionExecutor(BaseExecutor):
    def __init__(
        self,
        runtime: WorkerRuntime,
        env: Mapping[str, str] | None = None,
        ssh_client: Optional[SSHClient] = None,
        now_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.runtime = runtime
        self.env = dict(env or os.environ)
        self.now_fn = now_fn
        self.sleep_fn = sleep_fn

        self.poll_interval = float(self.env.get("DEUCALION_POLL_INTERVAL", "10"))
        self.sync_interval = float(self.env.get("DEUCALION_SYNC_INTERVAL", "15"))
        self.unreachable_grace_seconds = float(self.env.get("DEUCALION_UNREACHABLE_GRACE_SECONDS", "900"))
        self.unknown_state_timeout_seconds = float(self.env.get("DEUCALION_UNKNOWN_STATE_TIMEOUT_SECONDS", "300"))
        self.artifact_copy_retries = max(1, int(self.env.get("DEUCALION_ARTIFACT_COPY_RETRIES", "3")))
        self.artifact_copy_retry_backoff = max(
            0.0,
            float(self.env.get("DEUCALION_ARTIFACT_COPY_RETRY_BACKOFF", "0.5")),
        )
        self.dataset_copy_retries = max(1, int(self.env.get("DEUCALION_DATASET_COPY_RETRIES", "3")))
        self.dataset_copy_retry_backoff = max(
            0.0,
            float(self.env.get("DEUCALION_DATASET_COPY_RETRY_BACKOFF", "2.0")),
        )
        self.dataset_copy_timeout_seconds = max(
            60,
            int(self.env.get("DEUCALION_DATASET_COPY_TIMEOUT_SECONDS", "1800")),
        )
        self.container_workdir = self.env.get("DEUCALION_CONTAINER_WORKDIR", "/app").strip()
        self.sif_pull_procs = max(1, int(self.env.get("DEUCALION_SIF_PULL_PROCS", "1")))
        self.sif_mksquashfs_args = self.env.get("DEUCALION_SIF_MKSQUASHFS_ARGS", "").strip()
        self.sif_build_mode = self.env.get("DEUCALION_SIF_BUILD_MODE", "slurm").strip().lower() or "slurm"
        self.sif_build_timeout_seconds = max(
            60.0,
            float(self.env.get("DEUCALION_SIF_BUILD_TIMEOUT_SECONDS", "5400")),
        )
        self.sif_build_poll_interval = max(
            1.0,
            float(self.env.get("DEUCALION_SIF_BUILD_POLL_INTERVAL", "5")),
        )
        self.sif_build_time_limit = self.env.get("DEUCALION_SIF_BUILD_TIME", "").strip()
        self.sif_build_partition = self.env.get("DEUCALION_SIF_BUILD_PARTITION", "").strip()
        self.sif_build_account = self.env.get("DEUCALION_SIF_BUILD_ACCOUNT", "").strip()
        self.sif_build_cpus = max(1, int(self.env.get("DEUCALION_SIF_BUILD_CPUS", "2")))
        self.sif_build_mem_gb = max(1, int(self.env.get("DEUCALION_SIF_BUILD_MEM_GB", "8")))
        self.budget_refresh_interval = max(
            60.0,
            float(self.env.get("DEUCALION_BUDGET_REFRESH_INTERVAL_SECONDS", "3600")),
        )
        self._budget_snapshot: dict[str, Any] | None = None
        self._budget_refreshed_at: float | None = None
        self._budget_last_attempt_at = 0.0

        if ssh_client is None:
            ssh_client = SSHClient(
                SSHSettings(
                    host=self._require_env("DEUCALION_SSH_HOST"),
                    user=self._require_env("DEUCALION_SSH_USER"),
                    port=int(self.env.get("DEUCALION_SSH_PORT", "22")),
                    key_path=self._require_env("DEUCALION_SSH_KEY_PATH"),
                    known_hosts_path=self._require_env("DEUCALION_SSH_KNOWN_HOSTS"),
                )
            )
        self.ssh = ssh_client

    def _require_env(self, key: str) -> str:
        value = self.env.get(key, "").strip()
        if not value:
            raise RuntimeError(f"Missing required environment variable: {key}")
        return value

    @staticmethod
    def _parse_numeric(value: str) -> float | None:
        stripped = value.strip().replace(",", "")
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None

    @classmethod
    def _parse_billing_output(cls, output: str) -> dict[str, Any] | None:
        rows: list[dict[str, Any]] = []
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            delimiter = "│" if "│" in line else "|" if "|" in line else None
            if delimiter is None:
                continue
            parts = [part.strip() for part in line.strip(delimiter).split(delimiter)]
            if len(parts) != 4:
                continue
            if parts[0].lower() == "account":
                continue
            used = cls._parse_numeric(parts[1])
            limit = cls._parse_numeric(parts[2])
            pct = cls._parse_numeric(parts[3])
            if used is None or limit is None or pct is None:
                continue
            rows.append(
                {
                    "account": parts[0],
                    "used_hours": used,
                    "limit_hours": limit,
                    "used_percent": pct,
                }
            )
        if not rows:
            return None
        return {"accounts": rows}

    def _refresh_budget_snapshot(self) -> None:
        now = time.time()
        self._budget_last_attempt_at = now
        try:
            output = self.ssh.run("billing", timeout=60, check=False)
        except SSHCommandError as exc:
            _LOGGER.warning("Failed to refresh Deucalion budget snapshot: %s", exc)
            return
        parsed = self._parse_billing_output(output)
        if parsed is None:
            _LOGGER.warning("Unable to parse billing output for Deucalion budget")
            return
        self._budget_snapshot = parsed
        self._budget_refreshed_at = now

    def heartbeat_info(self) -> Dict[str, Any]:
        now = time.time()
        should_refresh = (
            self._budget_snapshot is None
            or (now - self._budget_last_attempt_at) >= self.budget_refresh_interval
        )
        if should_refresh:
            self._refresh_budget_snapshot()
        if self._budget_snapshot is None:
            return {}
        return {
            "budget": self._budget_snapshot,
            "budget_refreshed_at": self._budget_refreshed_at,
        }

    def _remote_root(self, cfg: DeucalionJobConfig) -> str:
        return cfg.remote_root.rstrip("/")

    def _local_config_path(self, config_path: str) -> Path:
        relative = config_path.lstrip("/")
        return Path(self.runtime.shared_dir) / relative

    def _load_job_yaml(self, local_config_path: Path) -> dict[str, Any]:
        if not local_config_path.exists():
            raise FileNotFoundError(f"Config file not found: {local_config_path}")
        with open(local_config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError("Job config must be a mapping (YAML object)")
        return data

    def _check_remote_path(self, path: str, kind: str = "e") -> None:
        self.ssh.run(f"test -{kind} {shlex.quote(path)}", timeout=30)

    def _sif_exists(self, sif_path: str) -> bool:
        return self._remote_exists(sif_path, kind="f")

    def _sif_version_marker_path(self, cfg: DeucalionJobConfig) -> str:
        marker_dir = posixpath.join(self._remote_root(cfg), ".opeva", "sif_versions")
        marker_name = f"{cfg.sif_path.strip('/').replace('/', '__')}.version"
        return posixpath.join(marker_dir, marker_name)

    def _read_remote_file_text(self, remote_path: str) -> str | None:
        output = self.ssh.run(
            f"if [ -f {shlex.quote(remote_path)} ]; then cat {shlex.quote(remote_path)}; fi",
            timeout=30,
            check=False,
        ).strip()
        return output or None

    def _write_remote_file_text(self, remote_path: str, content: str) -> None:
        remote_dir = posixpath.dirname(remote_path)
        self._ensure_remote_dir(remote_dir)
        self.ssh.run(
            f"printf %s {shlex.quote(content)} > {shlex.quote(remote_path)}",
            timeout=30,
        )

    def _image_ref_for_version(self, cfg: DeucalionJobConfig) -> str | None:
        if not cfg.sif_image:
            return None
        if not cfg.sif_version:
            return cfg.sif_image
        if "@" in cfg.sif_image:
            return cfg.sif_image

        image = cfg.sif_image
        last_segment = image.rsplit("/", 1)[-1]
        if ":" in last_segment:
            base, _tag = image.rsplit(":", 1)
            return f"{base}:{cfg.sif_version}"
        return f"{image}:{cfg.sif_version}"

    def _render_sif_build_script(
        self,
        cfg: DeucalionJobConfig,
        image_ref: str,
        cache_dir: str,
        tmp_dir: str,
        account: str,
        partition: str,
        time_limit: str,
        cpus: int,
        mem_gb: int,
        out_path: str,
        err_path: str,
    ) -> str:
        mksquashfs_line = ""
        if self.sif_mksquashfs_args:
            mksquashfs_line = f"APPTAINER_MKSQUASHFS_ARGS={shlex.quote(self.sif_mksquashfs_args)} "
        lines = [
            "#!/bin/bash",
            f"#SBATCH -J opeva_sif_{uuid.uuid4().hex[:8]}",
            f"#SBATCH -A {account}",
            f"#SBATCH -p {partition}",
            f"#SBATCH -t {time_limit}",
            f"#SBATCH --cpus-per-task={cpus}",
            f"#SBATCH --mem={mem_gb}G",
            f"#SBATCH -o {out_path}",
            f"#SBATCH -e {err_path}",
            "",
            "set -euo pipefail",
            "module purge >/dev/null 2>&1 || true",
            "module load singularity >/dev/null 2>&1 || true",
            f"mkdir -p {shlex.quote(posixpath.dirname(cfg.sif_path))}",
            f"mkdir -p {shlex.quote(cache_dir)}",
            f"mkdir -p {shlex.quote(tmp_dir)}",
            f"export APPTAINER_CACHEDIR={shlex.quote(cache_dir)}",
            f"export APPTAINER_TMPDIR={shlex.quote(tmp_dir)}",
            f"export APPTAINER_MKSQUASHFS_PROCS={self.sif_pull_procs}",
            f"export SINGULARITY_CACHEDIR={shlex.quote(cache_dir)}",
            f"export SINGULARITY_TMPDIR={shlex.quote(tmp_dir)}",
            f"export SINGULARITY_MKSQUASHFS_PROCS={self.sif_pull_procs}",
            (
                "if command -v apptainer >/dev/null 2>&1; then "
                f"{mksquashfs_line}apptainer build --force {shlex.quote(cfg.sif_path)} "
                f"docker://{shlex.quote(image_ref)}; "
                "elif command -v singularity >/dev/null 2>&1; then "
                f"singularity build --force {shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}; "
                "else "
                "echo 'Neither apptainer nor singularity found on PATH' >&2; exit 127; "
                "fi"
            ),
            f"test -f {shlex.quote(cfg.sif_path)}",
        ]
        return "\n".join(lines) + "\n"

    def _read_remote_tail(self, remote_path: str, lines: int = 80) -> str:
        return self.ssh.run(
            (
                f"if [ -f {shlex.quote(remote_path)} ]; then "
                f"tail -n {max(1, lines)} {shlex.quote(remote_path)}; "
                "fi"
            ),
            timeout=60,
            check=False,
        ).strip()

    def _ensure_remote_sif_via_slurm(
        self,
        cfg: DeucalionJobConfig,
        image_ref: str,
        marker_path: str,
        cache_dir: str,
        tmp_dir: str,
    ) -> None:
        remote_root = self._remote_root(cfg)
        build_root = posixpath.join(remote_root, ".opeva", "sif_builds")
        build_id = uuid.uuid4().hex[:10]
        build_dir = posixpath.join(build_root, build_id)
        self._ensure_remote_dir(build_dir)
        remote_script_path = posixpath.join(build_dir, "build_sif.sbatch")

        account = self.sif_build_account or cfg.profile.account
        partition = self.sif_build_partition or cfg.profile.partition
        time_limit = self.sif_build_time_limit or cfg.profile.time_limit
        cpus = self.sif_build_cpus
        mem_gb = self.sif_build_mem_gb
        out_path = posixpath.join(build_dir, "sif-build.out")
        err_path = posixpath.join(build_dir, "sif-build.err")
        script_text = self._render_sif_build_script(
            cfg=cfg,
            image_ref=image_ref,
            cache_dir=cache_dir,
            tmp_dir=tmp_dir,
            account=account,
            partition=partition,
            time_limit=time_limit,
            cpus=cpus,
            mem_gb=mem_gb,
            out_path=out_path,
            err_path=err_path,
        )
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tmp:
            tmp.write(script_text)
            tmp_path = Path(tmp.name)
        try:
            self.ssh.copy_to(tmp_path, remote_script_path, timeout=60, recursive=False)
        finally:
            tmp_path.unlink(missing_ok=True)

        slurm_job_id = sbatch_submit(self.ssh, remote_script_path=remote_script_path, remote_workdir=build_dir)
        deadline = self.now_fn() + self.sif_build_timeout_seconds
        while True:
            state = query_state(self.ssh, slurm_job_id)
            if state.state in ACTIVE_STATES or state.state == "UNKNOWN":
                if self.now_fn() > deadline:
                    scancel_job(self.ssh, slurm_job_id)
                    raise TimeoutError(
                        f"SIF build timed out (job={slurm_job_id}, state={state.state}) for docker://{image_ref}"
                    )
                self.sleep_fn(self.sif_build_poll_interval)
                continue
            if state.state == "COMPLETED" and self._sif_exists(cfg.sif_path):
                break
            out_tail = self._read_remote_tail(out_path)
            err_tail = self._read_remote_tail(err_path)
            details = "; ".join(
                part
                for part in [
                    f"state={state.state}",
                    f"exit_code={state.exit_code}",
                    f"stdout_tail={out_tail}" if out_tail else "",
                    f"stderr_tail={err_tail}" if err_tail else "",
                ]
                if part
            )
            raise FileNotFoundError(
                f"Failed to build SIF at {cfg.sif_path} from docker://{image_ref} via Slurm job {slurm_job_id}. {details}"
            )

        if cfg.sif_version:
            self._write_remote_file_text(marker_path, cfg.sif_version)

    def _ensure_remote_sif(self, cfg: DeucalionJobConfig) -> None:
        sif_exists = self._sif_exists(cfg.sif_path)
        marker_path = self._sif_version_marker_path(cfg)
        remote_version = self._read_remote_file_text(marker_path)
        version_changed = bool(cfg.sif_version) and cfg.sif_version != remote_version
        needs_refresh = (not sif_exists) or version_changed

        if not needs_refresh:
            return

        image_ref = self._image_ref_for_version(cfg)
        if not image_ref:
            reason = "version mismatch" if version_changed else "missing SIF"
            raise FileNotFoundError(
                f"Cannot refresh SIF ({reason}) at {cfg.sif_path}: no sif_image provided."
            )

        remote_root = self._remote_root(cfg)
        sif_dir = posixpath.dirname(cfg.sif_path)
        cache_dir = self.env.get(
            "DEUCALION_SIF_CACHE_DIR",
            posixpath.join(remote_root, ".opeva", "sif_cache"),
        )
        tmp_dir = self.env.get(
            "DEUCALION_SIF_TMP_DIR",
            posixpath.join(remote_root, ".opeva", "sif_tmp"),
        )
        self._ensure_remote_dir(sif_dir)
        self._ensure_remote_dir(cache_dir)
        self._ensure_remote_dir(tmp_dir)

        if self.sif_build_mode == "slurm":
            self._ensure_remote_sif_via_slurm(
                cfg=cfg,
                image_ref=image_ref,
                marker_path=marker_path,
                cache_dir=cache_dir,
                tmp_dir=tmp_dir,
            )
            return

        pull_env = (
            f"APPTAINER_CACHEDIR={shlex.quote(cache_dir)} "
            f"APPTAINER_TMPDIR={shlex.quote(tmp_dir)} "
            f"APPTAINER_MKSQUASHFS_PROCS={self.sif_pull_procs} "
            f"SINGULARITY_CACHEDIR={shlex.quote(cache_dir)} "
            f"SINGULARITY_TMPDIR={shlex.quote(tmp_dir)} "
            f"SINGULARITY_MKSQUASHFS_PROCS={self.sif_pull_procs}"
        )

        build_ok = False
        attempt_errors: list[str] = []
        mksquashfs_flag = (
            f"--mksquashfs-args {shlex.quote(self.sif_mksquashfs_args)} " if self.sif_mksquashfs_args else ""
        )

        build_attempts = [
            (
                "apptainer+mksquashfs-args",
                f"{pull_env} command -v apptainer >/dev/null 2>&1 && "
                f"apptainer build --force {mksquashfs_flag}{shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}",
            ),
            (
                "singularity+keep-layers",
                f"{pull_env} command -v singularity >/dev/null 2>&1 && "
                f"singularity build --force --keep-layers {shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}",
            ),
            (
                "singularity+build",
                f"{pull_env} command -v singularity >/dev/null 2>&1 && "
                f"singularity build --force {shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}",
            ),
            (
                "apptainer",
                f"{pull_env} command -v apptainer >/dev/null 2>&1 && "
                f"apptainer pull --force {shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}",
            ),
            (
                "singularity",
                f"{pull_env} command -v singularity >/dev/null 2>&1 && "
                f"singularity pull --force {shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}",
            ),
            (
                "module+apptainer",
                f"source /etc/profile >/dev/null 2>&1 || true; "
                f"source /etc/profile.d/modules.sh >/dev/null 2>&1 || true; "
                f"module load apptainer >/dev/null 2>&1 || true; "
                f"{pull_env} "
                f"command -v apptainer >/dev/null 2>&1 && "
                f"apptainer pull --force {shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}",
            ),
            (
                "module+singularity",
                f"source /etc/profile >/dev/null 2>&1 || true; "
                f"source /etc/profile.d/modules.sh >/dev/null 2>&1 || true; "
                f"module load singularity >/dev/null 2>&1 || true; "
                f"{pull_env} "
                f"command -v singularity >/dev/null 2>&1 && "
                f"singularity pull --force {shlex.quote(cfg.sif_path)} docker://{shlex.quote(image_ref)}",
            ),
        ]

        for attempt_name, build_cmd in build_attempts:
            try:
                self.ssh.run(build_cmd, timeout=1800, check=True)
            except SSHCommandError as exc:
                stderr = (exc.stderr or exc.stdout or str(exc)).strip().replace("\n", " | ")
                if stderr:
                    attempt_errors.append(f"{attempt_name}: {stderr}")
                else:
                    attempt_errors.append(f"{attempt_name}: command failed (rc={exc.returncode})")
                continue
            if self._sif_exists(cfg.sif_path):
                build_ok = True
                break
            attempt_errors.append(f"{attempt_name}: command completed but SIF path still missing")

        if not build_ok:
            details = "; ".join(attempt_errors) if attempt_errors else "no pull attempt details available"
            raise FileNotFoundError(
                f"Failed to build SIF at {cfg.sif_path} from docker://{image_ref}. Attempts: {details}"
            )

        if cfg.sif_version:
            self._write_remote_file_text(
                marker_path,
                cfg.sif_version,
            )

    def _remote_exists(self, path: str, kind: str = "e") -> bool:
        output = self.ssh.run(
            f"test -{kind} {shlex.quote(path)} && echo yes || true",
            timeout=30,
            check=False,
        )
        return output.strip() == "yes"

    def _ensure_remote_dir(self, path: str) -> None:
        self.ssh.run(f"mkdir -p {shlex.quote(path)}", timeout=30)

    def _remote_file_size(self, remote_path: str) -> int:
        output = self.ssh.run(
            f"if [ -f {shlex.quote(remote_path)} ]; then wc -c < {shlex.quote(remote_path)}; else echo 0; fi",
            timeout=30,
            check=False,
        ).strip()
        try:
            return int(output) if output else 0
        except ValueError:
            return 0

    def _read_remote_file_from(self, remote_path: str, start: int) -> str:
        return self.ssh.run(
            f"if [ -f {shlex.quote(remote_path)} ]; then tail -c +{start} {shlex.quote(remote_path)}; fi",
            timeout=60,
            check=False,
        )

    def _sync_remote_logs(self, remote_job_dir: str, local_log_path: Path, offsets: dict[str, int]) -> dict[str, int]:
        local_log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_log_path, "a", encoding="utf-8") as log_file:
            for name in ("slurm.out", "slurm.err"):
                remote_path = posixpath.join(remote_job_dir, name)
                prev_size = offsets.get(name, 0)
                current_size = self._remote_file_size(remote_path)
                if current_size <= 0:
                    offsets[name] = 0
                    continue
                # File rotated/truncated remotely; resync from beginning.
                start = 1 if current_size < prev_size else prev_size + 1
                if current_size == prev_size:
                    continue
                delta = self._read_remote_file_from(remote_path, start=start)
                if delta:
                    log_file.write(delta)
                    log_file.flush()
                offsets[name] = current_size
        return offsets

    def _sync_remote_progress_snapshot(
        self,
        remote_job_dir: str,
        remote_data_dir: str,
        job_id: str,
        local_job_dir: Path,
    ) -> None:
        local_progress_dir = local_job_dir / "progress"
        local_progress_dir.mkdir(parents=True, exist_ok=True)
        local_progress_path = local_progress_dir / "progress.json"

        candidate_paths = (
            posixpath.join(remote_data_dir, "jobs", job_id, "progress", "progress.json"),
            posixpath.join(remote_job_dir, "progress", "progress.json"),
        )
        for remote_path in candidate_paths:
            exists = self.ssh.run(f"test -f {shlex.quote(remote_path)} && echo yes || true", check=False)
            if exists.strip() != "yes":
                continue
            tmp_path = local_progress_path.with_name("progress.json.tmp")
            try:
                self.ssh.copy_from(remote_path, tmp_path, timeout=60, recursive=False)
                os.replace(tmp_path, local_progress_path)
                return
            except SSHCommandError as exc:
                _LOGGER.warning("Failed to sync remote progress snapshot from %s: %s", remote_path, exc)
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except OSError:
                    pass
                return

    def _sync_datasets(self, cfg: DeucalionJobConfig) -> tuple[list[str], list[str]]:
        synced: list[str] = []
        skipped: list[str] = []
        for rel_path in cfg.datasets:
            local_source = Path(self.runtime.shared_dir) / rel_path
            remote_target = posixpath.join(cfg.remote_root, rel_path)
            if not local_source.exists():
                raise FileNotFoundError(f"Dataset path not found in shared dir: {local_source}")
            if self._remote_exists(remote_target, kind="e"):
                skipped.append(rel_path)
                continue
            self._ensure_remote_dir(posixpath.dirname(remote_target))
            is_dir = local_source.is_dir()
            timeout = self.dataset_copy_timeout_seconds if is_dir else 300
            last_exc: SSHCommandError | None = None
            for attempt in range(1, self.dataset_copy_retries + 1):
                try:
                    self.ssh.copy_to(local_source, remote_target, timeout=timeout, recursive=is_dir)
                    last_exc = None
                    break
                except SSHCommandError as exc:
                    last_exc = exc
                    _LOGGER.warning(
                        "Dataset sync failed for %s -> %s (attempt %d/%d): %s",
                        local_source,
                        remote_target,
                        attempt,
                        self.dataset_copy_retries,
                        exc,
                    )
                    if attempt < self.dataset_copy_retries and self.dataset_copy_retry_backoff > 0:
                        backoff = min(
                            self.dataset_copy_retry_backoff * (2 ** (attempt - 1)),
                            _ARTIFACT_SYNC_RETRY_MAX_BACKOFF_SECONDS * 6,
                        )
                        self.sleep_fn(backoff)
            if last_exc is not None:
                detail = (last_exc.stderr or last_exc.stdout or str(last_exc)).strip().replace("\n", " | ")
                raise RuntimeError(
                    f"Failed to sync dataset '{rel_path}' to '{remote_target}': {detail or 'unknown SCP error'}"
                ) from last_exc
            synced.append(rel_path)
        return synced, skipped

    def _ensure_datasets_symlink(self, remote_root: str, remote_job_dir: str) -> None:
        datasets_root = posixpath.join(remote_root, "datasets")
        link_path = posixpath.join(remote_job_dir, "data", "datasets")
        self._ensure_remote_dir(datasets_root)
        self._ensure_remote_dir(posixpath.dirname(link_path))
        self.ssh.run(
            f"ln -sfn {shlex.quote(datasets_root)} {shlex.quote(link_path)}",
            timeout=30,
        )

    @staticmethod
    def _derive_image_version(image_ref: str) -> str | None:
        ref = image_ref.strip()
        if not ref:
            return None
        if "@" in ref:
            digest = ref.split("@", 1)[1].strip()
            return digest or None
        last_segment = ref.rsplit("/", 1)[-1]
        if ":" in last_segment:
            return ref.rsplit(":", 1)[1].strip() or None
        return None

    def _apply_job_image(self, cfg: DeucalionJobConfig, job: Dict[str, Any]) -> DeucalionJobConfig:
        image_ref = str(job.get("image") or "").strip()
        if not image_ref:
            raise ValueError("Missing job image in payload for deucalion executor")

        cfg.sif_image = image_ref
        derived_version = self._derive_image_version(image_ref)
        # The job payload image is authoritative; never fall back to YAML versioning.
        cfg.sif_version = derived_version
        return cfg

    def _build_singularity_command(self, cfg: DeucalionJobConfig, remote_data_dir: str, command: str) -> str:
        command_text = command.strip()
        if not command_text:
            raise ValueError("Empty job command")
        if cfg.command_mode == "exec":
            parts = shlex.split(command_text)
            if not parts or parts[0].startswith("-"):
                raise ValueError(
                    "execution.deucalion.command_mode=exec requires an explicit executable in the command"
                )
        pwd_opt = f"--pwd {shlex.quote(self.container_workdir)} " if self.container_workdir else ""
        return (
            f"singularity {cfg.command_mode} "
            f"{pwd_opt}"
            f"--bind {shlex.quote(remote_data_dir)}:/data "
            f"{shlex.quote(cfg.sif_path)} "
            f"{command_text}"
        )

    def _sync_remote_artifacts(
        self,
        remote_job_dir: str,
        remote_data_dir: str,
        job_id: str,
        local_job_dir: Path,
    ) -> dict[str, Any]:
        local_job_dir.mkdir(parents=True, exist_ok=True)
        summary: dict[str, Any] = {
            "had_failure": False,
            "folders": {},
        }
        for folder in ("results", "progress"):
            folder_summary = {
                "status": "missing",
                "source": None,
                "attempts": 0,
                "errors": [],
            }
            candidate_paths = (
                posixpath.join(remote_data_dir, "jobs", job_id, folder),
                posixpath.join(remote_job_dir, folder),
            )
            found_existing = False
            for remote_path in candidate_paths:
                exists = self.ssh.run(f"test -d {shlex.quote(remote_path)} && echo yes || true", check=False)
                if exists.strip() != "yes":
                    continue
                found_existing = True
                for attempt in range(1, self.artifact_copy_retries + 1):
                    try:
                        self.ssh.copy_from(remote_path, local_job_dir, recursive=True)
                        folder_summary["status"] = "synced"
                        folder_summary["source"] = remote_path
                        folder_summary["attempts"] = attempt
                        break
                    except SSHCommandError as exc:
                        folder_summary["source"] = remote_path
                        folder_summary["attempts"] = attempt
                        folder_summary["errors"].append(exc.stderr or str(exc))
                        if attempt < self.artifact_copy_retries:
                            backoff = min(
                                self.artifact_copy_retry_backoff * (2 ** (attempt - 1)),
                                _ARTIFACT_SYNC_RETRY_MAX_BACKOFF_SECONDS,
                            )
                            if backoff > 0:
                                self.sleep_fn(backoff)
                        _LOGGER.warning(
                            "Failed to sync remote '%s' from %s (attempt %d/%d): %s",
                            folder,
                            remote_path,
                            attempt,
                            self.artifact_copy_retries,
                            exc,
                        )
                if folder_summary["status"] == "synced":
                    break

            if folder_summary["status"] != "synced":
                if found_existing:
                    folder_summary["status"] = "failed"
                    summary["had_failure"] = True
                    _LOGGER.error("Failed to sync remote '%s' artifacts for job %s", folder, job_id)
                else:
                    folder_summary["status"] = "missing"
                    _LOGGER.debug("No remote '%s' artifacts found for job %s", folder, job_id)
            summary["folders"][folder] = folder_summary
        return summary

    def _render_sbatch_script(
        self,
        cfg: DeucalionJobConfig,
        job_id: str,
        remote_job_dir: str,
        remote_data_dir: str,
        command: str,
        env_vars: dict[str, Any],
    ) -> str:
        profile = cfg.profile
        lines = [
            "#!/bin/bash",
            f"#SBATCH -J opeva_{job_id[:8]}",
            f"#SBATCH -A {profile.account}",
            f"#SBATCH -p {profile.partition}",
            f"#SBATCH -t {profile.time_limit}",
            f"#SBATCH --cpus-per-task={profile.cpus_per_task}",
            f"#SBATCH --mem={profile.mem_gb}G",
            f"#SBATCH -o {posixpath.join(remote_job_dir, 'slurm.out')}",
            f"#SBATCH -e {posixpath.join(remote_job_dir, 'slurm.err')}",
        ]
        if profile.gpus > 0:
            lines.append(f"#SBATCH --gpus={profile.gpus}")
        lines.extend(
            [
                "",
                "set -euo pipefail",
                "module purge >/dev/null 2>&1 || true",
            ]
        )
        for module_name in profile.modules:
            lines.append(f"module load {shlex.quote(module_name)}")
        lines.extend(
            [
                f"mkdir -p {shlex.quote(posixpath.join(remote_job_dir, 'results'))}",
                f"mkdir -p {shlex.quote(posixpath.join(remote_job_dir, 'progress'))}",
            ]
        )
        for key, value in env_vars.items():
            if value is None:
                continue
            lines.append(f"export {key}={shlex.quote(str(value))}")
        lines.append(self._build_singularity_command(cfg=cfg, remote_data_dir=remote_data_dir, command=command))
        return "\n".join(lines) + "\n"

    def _map_terminal_status(self, stop_reason: str | None, state: SlurmState) -> tuple[str, int | None, str | None]:
        if stop_reason == "canceled":
            return "canceled", state.exit_code, None
        if stop_reason == "stop_requested":
            return "stopped", state.exit_code, None

        if state.state == "COMPLETED" and (state.exit_code in (None, 0)):
            return "finished", state.exit_code, None
        if state.state == "CANCELLED":
            return "failed", state.exit_code, "slurm_cancelled"
        return "failed", state.exit_code, f"slurm_{state.state.lower()}"

    def run_job(self, job: Dict[str, Any]) -> None:
        job_id = job["job_id"]
        config_path = str(job["config_path"]).lstrip("/")
        job_name = str(job.get("job_name", job_id))
        command = str(job.get("command") or self.runtime._build_command(job_id, config_path))

        local_config_path = self._local_config_path(config_path)
        local_log_path = self.runtime._prepare_log_file(job_id)
        local_job_dir = local_log_path.parent.parent

        slurm_job_id: str | None = None
        degraded_since: float | None = None
        unknown_since: float | None = None
        stop_reason: str | None = None
        last_status = "dispatched"
        datasets_synced: list[str] = []
        datasets_skipped: list[str] = []
        command_mode = "run"
        log_offsets = {"slurm.out": 0, "slurm.err": 0}

        try:
            self.runtime._mark_active_job(job_id)
            job_yaml = self._load_job_yaml(local_config_path)
            cfg = resolve_deucalion_job_config(job_yaml, env=self.env)
            cfg = self._apply_job_image(cfg, job)
            remote_root = self._remote_root(cfg)
            command_mode = cfg.command_mode

            remote_job_dir = posixpath.join(remote_root, "runs", job_id)
            remote_data_dir = posixpath.join(remote_job_dir, "data")
            remote_cfg_path = posixpath.join(remote_data_dir, config_path)
            remote_script_path = posixpath.join(remote_job_dir, "run.sbatch")

            # Preflight
            self._check_remote_path(remote_root, kind="d")
            self._ensure_remote_sif(cfg)
            for path in cfg.required_paths:
                self._check_remote_path(path, kind="e")

            datasets_synced, datasets_skipped = self._sync_datasets(cfg)

            self.ssh.run(
                f"mkdir -p {shlex.quote(remote_job_dir)} "
                f"{shlex.quote(posixpath.dirname(remote_cfg_path))}",
                timeout=30,
            )
            self._ensure_datasets_symlink(remote_root=remote_root, remote_job_dir=remote_job_dir)
            self.ssh.copy_to(local_config_path, remote_cfg_path, timeout=60)

            script = self._render_sbatch_script(
                cfg=cfg,
                job_id=job_id,
                remote_job_dir=remote_job_dir,
                remote_data_dir=remote_data_dir,
                command=command,
                env_vars=job.get("env", {}) or {},
            )
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".sbatch", delete=False) as tmp:
                tmp.write(script)
                tmp_path = tmp.name
            try:
                self.ssh.copy_to(tmp_path, remote_script_path, timeout=60)
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

            slurm_job_id = sbatch_submit(self.ssh, remote_script_path=remote_script_path, remote_workdir=remote_job_dir)
            details = {
                "slurm_job_id": slurm_job_id,
                "slurm_state": "PENDING",
                "executor": "deucalion",
                "command_mode": command_mode,
                "datasets_synced": datasets_synced,
                "datasets_skipped": datasets_skipped,
                "image": cfg.sif_image,
            }
            self.runtime._post_status(job_id, "dispatched", details=details, container_name=job_name)
            last_status = "dispatched"

            next_sync = 0.0
            while True:
                now = self.now_fn()
                try:
                    if self.runtime.status_poll_interval > 0:
                        backend_status = self.runtime._fetch_status(job_id)
                        if backend_status in _BACKEND_STOP_STATES and stop_reason is None:
                            stop_reason = backend_status
                            if slurm_job_id:
                                scancel_job(self.ssh, slurm_job_id)

                    state = query_state(self.ssh, slurm_job_id)
                    degraded_since = None
                    if state.state != "UNKNOWN":
                        unknown_since = None

                    details = {
                        "slurm_job_id": slurm_job_id,
                        "slurm_state": state.state,
                        "executor": "deucalion",
                        "command_mode": command_mode,
                        "datasets_synced": datasets_synced,
                        "datasets_skipped": datasets_skipped,
                        "image": cfg.sif_image,
                    }
                    if stop_reason:
                        details["backend_stop_reason"] = stop_reason

                    if now >= next_sync:
                        log_offsets = self._sync_remote_logs(remote_job_dir, local_log_path, log_offsets)
                        self._sync_remote_progress_snapshot(
                            remote_job_dir=remote_job_dir,
                            remote_data_dir=remote_data_dir,
                            job_id=job_id,
                            local_job_dir=local_job_dir,
                        )
                        next_sync = now + self.sync_interval

                    if state.state == "UNKNOWN":
                        if unknown_since is None:
                            unknown_since = now
                        details["unknown_since"] = unknown_since
                        if stop_reason in _BACKEND_OVERRIDE_NO_POST:
                            self.sleep_fn(self.poll_interval)
                            continue
                        if (now - unknown_since) > self.unknown_state_timeout_seconds:
                            self.runtime._post_status(
                                job_id,
                                "failed",
                                error="slurm_unknown_timeout",
                                details=details,
                            )
                            return
                        report_status = "running" if last_status == "running" else "dispatched"
                        self.runtime._post_status(job_id, report_status, details=details)
                        last_status = report_status
                        self.sleep_fn(self.poll_interval)
                        continue

                    if state.state in ACTIVE_STATES:
                        if stop_reason in _BACKEND_OVERRIDE_NO_POST:
                            self.sleep_fn(self.poll_interval)
                            continue
                        running_states = {"RUNNING", "COMPLETING", "STAGE_OUT"}
                        report_status = "running" if state.state in running_states else "dispatched"
                        self.runtime._post_status(job_id, report_status, details=details)
                        last_status = report_status
                        self.sleep_fn(self.poll_interval)
                        continue

                    final_status, exit_code, error = self._map_terminal_status(stop_reason, state)
                    self._sync_remote_logs(remote_job_dir, local_log_path, log_offsets)
                    self._sync_remote_progress_snapshot(
                        remote_job_dir=remote_job_dir,
                        remote_data_dir=remote_data_dir,
                        job_id=job_id,
                        local_job_dir=local_job_dir,
                    )
                    artifact_sync = self._sync_remote_artifacts(
                        remote_job_dir=remote_job_dir,
                        remote_data_dir=remote_data_dir,
                        job_id=job_id,
                        local_job_dir=local_job_dir,
                    )
                    details["artifact_sync"] = artifact_sync
                    if stop_reason in _BACKEND_OVERRIDE_NO_POST:
                        _LOGGER.info(
                            "Skipping terminal status post for job %s; backend already moved to '%s'",
                            job_id,
                            stop_reason,
                        )
                        return
                    if final_status == "finished" and artifact_sync.get("had_failure"):
                        final_status = "failed"
                        error = "artifact_sync_failed"
                    self.runtime._post_status(job_id, final_status, exit_code=exit_code, error=error, details=details)
                    return
                except SSHCommandError as exc:
                    if degraded_since is None:
                        degraded_since = now
                    elapsed = now - degraded_since
                    details = {
                        "slurm_job_id": slurm_job_id,
                        "connectivity": "degraded",
                        "executor": "deucalion",
                        "command_mode": command_mode,
                        "datasets_synced": datasets_synced,
                        "datasets_skipped": datasets_skipped,
                        "error": exc.stderr or str(exc),
                    }
                    if unknown_since is not None:
                        details["unknown_since"] = unknown_since
                    if stop_reason not in _BACKEND_OVERRIDE_NO_POST:
                        self.runtime._post_status(job_id, last_status, details=details)
                    if elapsed > self.unreachable_grace_seconds:
                        if stop_reason not in _BACKEND_OVERRIDE_NO_POST:
                            self.runtime._post_status(
                                job_id,
                                "failed",
                                error="deucalion_unreachable_timeout",
                                details={
                                    "slurm_job_id": slurm_job_id,
                                    "connectivity": "down",
                                    "executor": "deucalion",
                                    "command_mode": command_mode,
                                    "datasets_synced": datasets_synced,
                                    "datasets_skipped": datasets_skipped,
                                },
                            )
                        return
                    self.sleep_fn(self.poll_interval)
                except Exception as exc:
                    _LOGGER.exception("Unexpected Deucalion execution failure for job %s", job_id)
                    self.runtime._post_status(
                        job_id,
                        "failed",
                        error=str(exc),
                        details={
                            "slurm_job_id": slurm_job_id,
                            "executor": "deucalion",
                            "command_mode": command_mode,
                            "datasets_synced": datasets_synced,
                            "datasets_skipped": datasets_skipped,
                        },
                    )
                    return
        except Exception as exc:
            _LOGGER.exception("Failed to submit Deucalion job %s", job_id)
            self.runtime._post_status(
                job_id,
                "failed",
                error=str(exc),
                details={
                    "executor": "deucalion",
                    "command_mode": command_mode,
                    "datasets_synced": datasets_synced,
                    "datasets_skipped": datasets_skipped,
                },
            )
        finally:
            self.runtime._mark_active_job(None)
            self.runtime._send_heartbeat(force=True)
