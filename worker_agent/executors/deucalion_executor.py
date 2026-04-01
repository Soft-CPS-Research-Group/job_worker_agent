from __future__ import annotations

import logging
import os
import posixpath
import re
import shutil
import shlex
import subprocess
import tempfile
import time
import traceback
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


class PreflightInterrupted(RuntimeError):
    def __init__(self, *, status: str, stage: str):
        self.status = status
        self.stage = stage
        super().__init__(f"Preflight interrupted by backend status '{status}' during stage '{stage}'")


class SIFArtifactNotFound(FileNotFoundError):
    """Raised when the selected image tag has no published SIF artifact."""


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
        self.dataset_sync_mode = self.env.get("DEUCALION_DATASET_SYNC_MODE", "always").strip().lower()
        if self.dataset_sync_mode not in {"always", "exists"}:
            _LOGGER.warning(
                "Invalid DEUCALION_DATASET_SYNC_MODE=%r. Falling back to 'always'.",
                self.dataset_sync_mode,
            )
            self.dataset_sync_mode = "always"
        self.dataset_copy_timeout_seconds = max(
            60,
            int(self.env.get("DEUCALION_DATASET_COPY_TIMEOUT_SECONDS", "1800")),
        )
        self.container_workdir = self.env.get("DEUCALION_CONTAINER_WORKDIR", "/app").strip()
        self.sif_repository = self.env.get("DEUCALION_SIF_REPOSITORY", "calof/opeva_simulator_sif").strip().strip("/")
        self.sif_registry = self.env.get("DEUCALION_SIF_REGISTRY", "docker.io").strip().rstrip("/")
        self.sif_remote_cache_dir_env = self.env.get("DEUCALION_SIF_REMOTE_CACHE_DIR", "").strip()
        self.sif_local_cache_dir = Path(
            self.env.get("DEUCALION_SIF_LOCAL_CACHE_DIR", "/tmp/opeva_sif_artifacts")
        ).resolve()
        self.sif_pull_timeout_seconds = max(
            60.0,
            float(self.env.get("DEUCALION_SIF_PULL_TIMEOUT_SECONDS", "1800")),
        )
        self.sif_pull_retries = max(1, int(self.env.get("DEUCALION_SIF_PULL_RETRIES", "3")))
        self.sif_pull_retry_backoff = max(
            0.0,
            float(self.env.get("DEUCALION_SIF_PULL_RETRY_BACKOFF", "5.0")),
        )
        self.sif_lock_wait_timeout_seconds = max(
            10.0,
            float(self.env.get("DEUCALION_SIF_LOCK_WAIT_TIMEOUT_SECONDS", "900")),
        )
        self.sif_lock_poll_interval = max(
            0.5,
            float(self.env.get("DEUCALION_SIF_LOCK_POLL_INTERVAL", "2.0")),
        )
        self.sif_lock_stale_seconds = max(
            30.0,
            float(self.env.get("DEUCALION_SIF_LOCK_STALE_SECONDS", "1800")),
        )
        self.preflight_status_update_interval = max(
            5.0,
            float(self.env.get("DEUCALION_PREFLIGHT_STATUS_UPDATE_INTERVAL", "20")),
        )
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

    @staticmethod
    def _sanitize_sif_tag(tag: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", tag.strip())
        sanitized = sanitized.strip(".-")
        if not sanitized:
            raise ValueError(f"Invalid image tag for SIF cache path: {tag!r}")
        return sanitized

    def _remote_sif_cache_dir(self, cfg: DeucalionJobConfig) -> str:
        if self.sif_remote_cache_dir_env:
            return self.sif_remote_cache_dir_env
        return posixpath.join(self._remote_root(cfg), "images", "cache")

    def _remote_sif_lock_root(self, cfg: DeucalionJobConfig) -> str:
        return posixpath.join(self._remote_sif_cache_dir(cfg), ".locks")

    def _remote_sif_path_for_tag(self, cfg: DeucalionJobConfig, tag: str) -> str:
        return posixpath.join(self._remote_sif_cache_dir(cfg), f"{self._sanitize_sif_tag(tag)}.sif")

    def _remote_sif_lock_path(self, cfg: DeucalionJobConfig, tag: str) -> str:
        lock_name = f"{self._sanitize_sif_tag(tag)}.lock"
        return posixpath.join(self._remote_sif_lock_root(cfg), lock_name)

    def _sif_artifact_ref_for_tag(self, tag: str) -> str:
        repository = self.sif_repository
        if not repository:
            raise RuntimeError("Missing DEUCALION_SIF_REPOSITORY")
        first = repository.split("/", 1)[0]
        has_registry_prefix = "." in first or ":" in first or first == "localhost"
        if has_registry_prefix:
            return f"{repository}:{tag}"
        return f"{self.sif_registry}/{repository}:{tag}"

    def _cleanup_stale_remote_lock(self, lock_path: str) -> None:
        max_age = int(self.sif_lock_stale_seconds)
        self.ssh.run(
            (
                f"if [ -d {shlex.quote(lock_path)} ]; then "
                f"now=$(date +%s); "
                f"mtime=$(stat -c %Y {shlex.quote(lock_path)} 2>/dev/null || echo 0); "
                "age=$((now-mtime)); "
                f"if [ \"$age\" -ge {max_age} ]; then rmdir {shlex.quote(lock_path)} >/dev/null 2>&1 || true; fi; "
                "fi"
            ),
            timeout=30,
            check=False,
        )

    def _try_acquire_remote_lock(self, lock_path: str) -> bool:
        output = self.ssh.run(
            f"mkdir {shlex.quote(lock_path)} >/dev/null 2>&1 && echo acquired || true",
            timeout=30,
            check=False,
        )
        return output.strip() == "acquired"

    def _release_remote_lock(self, lock_path: str) -> None:
        self.ssh.run(f"rmdir {shlex.quote(lock_path)} >/dev/null 2>&1 || true", timeout=30, check=False)

    def _acquire_remote_sif_lock(
        self,
        *,
        cfg: DeucalionJobConfig,
        tag: str,
        remote_sif_path: str,
        local_log_path: Path | None = None,
    ) -> bool:
        lock_path = self._remote_sif_lock_path(cfg, tag)
        deadline = self.now_fn() + self.sif_lock_wait_timeout_seconds
        while self.now_fn() <= deadline:
            if self._sif_exists(remote_sif_path):
                return False
            self._cleanup_stale_remote_lock(lock_path)
            if self._try_acquire_remote_lock(lock_path):
                return True
            self.sleep_fn(self.sif_lock_poll_interval)
        if local_log_path is not None:
            self._append_local_log(local_log_path, f"Timed out waiting for remote SIF lock {lock_path}")
        raise TimeoutError(
            f"Timed out waiting for remote SIF lock for tag '{tag}' at {lock_path}"
        )

    @staticmethod
    def _is_transient_sif_pull_error(error_message: str) -> bool:
        message = (error_message or "").lower()
        transient_tokens = (
            "i/o timeout",
            "dial tcp",
            "connection timed out",
            "connection reset by peer",
            "temporary failure in name resolution",
            "tls handshake timeout",
            "context deadline exceeded",
            "no route to host",
            "network is unreachable",
            "too many requests",
            "rate limit",
        )
        return any(token in message for token in transient_tokens)

    @staticmethod
    def _resolve_pulled_sif_path(artifact_dir: Path) -> Path:
        sif_files = sorted(path for path in artifact_dir.rglob("*.sif") if path.is_file())
        if len(sif_files) == 1:
            return sif_files[0]
        if len(sif_files) > 1:
            for candidate in sif_files:
                if candidate.name == "simulator.sif":
                    return candidate
            return sif_files[0]
        all_files = sorted(path for path in artifact_dir.rglob("*") if path.is_file())
        if len(all_files) == 1:
            return all_files[0]
        raise FileNotFoundError(f"No SIF file found in pulled artifact directory: {artifact_dir}")

    def _pull_sif_artifact_local(self, *, tag: str, destination: Path) -> None:
        oras_binary = shutil.which("oras")
        if not oras_binary:
            raise RuntimeError("oras CLI not found in worker container PATH")

        destination.parent.mkdir(parents=True, exist_ok=True)
        artifact_ref = self._sif_artifact_ref_for_tag(tag)
        for attempt in range(1, self.sif_pull_retries + 1):
            with tempfile.TemporaryDirectory(prefix="opeva-sif-pull-") as tmp_dir:
                tmp_path = Path(tmp_dir)
                cmd = [oras_binary, "pull", artifact_ref, "--output", str(tmp_path)]
                try:
                    completed = subprocess.run(
                        cmd,
                        text=True,
                        capture_output=True,
                        timeout=self.sif_pull_timeout_seconds,
                    )
                except subprocess.TimeoutExpired as exc:
                    output = f"oras pull timeout for {artifact_ref}: {exc}"
                    if attempt >= self.sif_pull_retries:
                        raise RuntimeError(output) from exc
                    backoff = min(120.0, self.sif_pull_retry_backoff * (2 ** (attempt - 1)))
                    if backoff > 0:
                        self.sleep_fn(backoff)
                    continue
                output = "\n".join(part for part in [completed.stdout, completed.stderr] if part).strip()
                if completed.returncode == 0:
                    source = self._resolve_pulled_sif_path(tmp_path)
                    tmp_dest = destination.with_suffix(destination.suffix + ".tmp")
                    shutil.copyfile(source, tmp_dest)
                    os.replace(tmp_dest, destination)
                    return

            normalized_output = output.lower()
            if (
                "not found" in normalized_output
                or "manifest unknown" in normalized_output
                or "name unknown" in normalized_output
            ):
                raise SIFArtifactNotFound(
                    f"sif_not_published_for_tag: artifact {artifact_ref} is not available"
                )
            if attempt >= self.sif_pull_retries or not self._is_transient_sif_pull_error(output):
                raise RuntimeError(f"Failed to pull SIF artifact {artifact_ref}: {output or 'unknown error'}")
            backoff = min(120.0, self.sif_pull_retry_backoff * (2 ** (attempt - 1)))
            if backoff > 0:
                self.sleep_fn(backoff)

    def _ensure_remote_sif(
        self,
        cfg: DeucalionJobConfig,
        job_id: str | None = None,
        local_log_path: Path | None = None,
    ) -> None:
        if not cfg.sif_version:
            raise ValueError("Missing image tag for Deucalion SIF resolution")
        tag = self._sanitize_sif_tag(cfg.sif_version)
        remote_sif_path = cfg.sif_path or self._remote_sif_path_for_tag(cfg, tag)
        cfg.sif_path = remote_sif_path

        cache_dir = posixpath.dirname(remote_sif_path)
        lock_root = self._remote_sif_lock_root(cfg)
        self._ensure_remote_dir(cache_dir)
        self._ensure_remote_dir(lock_root)

        if self._sif_exists(remote_sif_path):
            if local_log_path is not None:
                self._append_local_log(local_log_path, f"SIF cache hit for tag {tag} at {remote_sif_path}")
            return

        acquired = self._acquire_remote_sif_lock(
            cfg=cfg,
            tag=tag,
            remote_sif_path=remote_sif_path,
            local_log_path=local_log_path,
        )
        if not acquired:
            if local_log_path is not None:
                self._append_local_log(local_log_path, f"SIF cache filled by another worker for tag {tag}")
            return

        lock_path = self._remote_sif_lock_path(cfg, tag)
        try:
            if self._sif_exists(remote_sif_path):
                if local_log_path is not None:
                    self._append_local_log(local_log_path, f"SIF cache hit for tag {tag} after lock acquire")
                return
            if local_log_path is not None:
                self._append_local_log(local_log_path, f"Pulling SIF artifact for tag {tag} into local cache")
            local_sif_path = self.sif_local_cache_dir / f"{tag}.sif"
            self._pull_sif_artifact_local(tag=tag, destination=local_sif_path)

            remote_tmp = f"{remote_sif_path}.part-{uuid.uuid4().hex[:8]}"
            try:
                if local_log_path is not None:
                    self._append_local_log(local_log_path, f"Uploading SIF to Deucalion cache: {remote_sif_path}")
                self.ssh.copy_to(local_sif_path, remote_tmp, timeout=int(self.sif_pull_timeout_seconds), recursive=False)
                self.ssh.run(
                    f"mv {shlex.quote(remote_tmp)} {shlex.quote(remote_sif_path)}",
                    timeout=60,
                    check=True,
                )
            finally:
                self.ssh.run(f"rm -f {shlex.quote(remote_tmp)}", timeout=30, check=False)
            if not self._sif_exists(remote_sif_path):
                raise FileNotFoundError(f"Uploaded SIF missing at remote path: {remote_sif_path}")
            if local_log_path is not None:
                self._append_local_log(local_log_path, f"SIF published in Deucalion cache for tag {tag}")
        finally:
            self._release_remote_lock(lock_path)

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

    def _append_local_log(self, local_log_path: Path, message: str) -> None:
        local_log_path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(local_log_path, "a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] {message.rstrip()}\n")

    def _poll_backend_stop_status(self, job_id: str) -> str | None:
        if self.runtime.status_poll_interval <= 0:
            return None
        backend_status = self.runtime._fetch_status(job_id)
        if backend_status in _BACKEND_STOP_STATES:
            return backend_status
        return None

    def _resolve_interrupted_preflight_status(self, status: str) -> str | None:
        if status == "stop_requested":
            return "stopped"
        if status == "canceled":
            return "canceled"
        return None

    def _maybe_interrupt_preflight(self, *, job_id: str, stage: str, last_probe_at: float) -> float:
        now = self.now_fn()
        if (now - last_probe_at) < self.preflight_status_update_interval:
            return last_probe_at
        status = self._poll_backend_stop_status(job_id)
        if status in _BACKEND_STOP_STATES:
            raise PreflightInterrupted(status=status, stage=stage)
        return now

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

    def _sync_remote_job_info_snapshot(
        self,
        remote_job_dir: str,
        remote_data_dir: str,
        job_id: str,
        local_job_dir: Path,
    ) -> dict[str, Any]:
        local_job_dir.mkdir(parents=True, exist_ok=True)
        local_info_path = local_job_dir / "job_info.json"
        summary: dict[str, Any] = {"status": "missing", "source": None, "error": None}

        candidate_paths = (
            posixpath.join(remote_data_dir, "jobs", job_id, "job_info.json"),
            posixpath.join(remote_job_dir, "job_info.json"),
        )
        for remote_path in candidate_paths:
            exists = self.ssh.run(f"test -f {shlex.quote(remote_path)} && echo yes || true", check=False)
            if exists.strip() != "yes":
                continue
            tmp_path = local_info_path.with_name("job_info.json.tmp")
            try:
                self.ssh.copy_from(remote_path, tmp_path, timeout=60, recursive=False)
                os.replace(tmp_path, local_info_path)
                summary["status"] = "synced"
                summary["source"] = remote_path
                return summary
            except SSHCommandError as exc:
                summary["status"] = "failed"
                summary["source"] = remote_path
                summary["error"] = exc.stderr or str(exc)
                _LOGGER.warning("Failed to sync remote job_info snapshot from %s: %s", remote_path, exc)
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except OSError:
                    pass
                return summary
        return summary

    def _sync_datasets(self, cfg: DeucalionJobConfig) -> tuple[list[str], list[str]]:
        synced: list[str] = []
        skipped: list[str] = []
        for rel_path in cfg.datasets:
            local_source = Path(self.runtime.shared_dir) / rel_path
            remote_target = posixpath.join(cfg.remote_root, rel_path)
            if not local_source.exists():
                raise FileNotFoundError(f"Dataset path not found in shared dir: {local_source}")
            if self.dataset_sync_mode == "exists" and self._remote_exists(remote_target, kind="e"):
                skipped.append(rel_path)
                continue
            if self.dataset_sync_mode == "always":
                # Force a full refresh to avoid stale/partial remote datasets.
                self.ssh.run(f"rm -rf {shlex.quote(remote_target)}", timeout=60, check=False)
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
            # Digest refs are intentionally not supported for Deucalion SIF
            # resolution because OCI artifact tags are resolved by image tag.
            return None
        last_segment = ref.rsplit("/", 1)[-1]
        if ":" in last_segment:
            return ref.rsplit(":", 1)[1].strip() or None
        return None

    def _apply_job_image(self, cfg: DeucalionJobConfig, job: Dict[str, Any]) -> DeucalionJobConfig:
        image_ref = str(job.get("image") or "").strip()
        if not image_ref:
            raise ValueError("Missing job image in payload for deucalion executor")

        tag = self._derive_image_version(image_ref)
        if not tag:
            raise ValueError(
                "Deucalion requires a tagged Docker image (example: calof/opeva_simulator:sha-xxxx)"
            )
        tag = self._sanitize_sif_tag(tag)
        cfg.sif_image = image_ref
        cfg.sif_version = tag
        cfg.sif_path = self._remote_sif_path_for_tag(cfg, tag)
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
        summary["job_info"] = self._sync_remote_job_info_snapshot(
            remote_job_dir=remote_job_dir,
            remote_data_dir=remote_data_dir,
            job_id=job_id,
            local_job_dir=local_job_dir,
        )
        if isinstance(summary["job_info"], dict) and summary["job_info"].get("status") == "failed":
            summary["had_failure"] = True
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

    @staticmethod
    def _inject_slurm_queue_details(details: dict[str, Any], state: SlurmState) -> None:
        mapping = {
            "slurm_partition": state.partition,
            "slurm_reason": state.reason,
            "slurm_submit_time": state.submit_time,
            "slurm_start_time": state.start_time,
            "slurm_elapsed": state.elapsed,
            "slurm_time_left": state.time_left,
            "slurm_priority": state.priority,
            "slurm_user": state.user,
            "slurm_nodes": state.nodes,
            "slurm_cpus": state.cpus,
            "slurm_queue_position": state.queue_position,
            "slurm_jobs_ahead": state.jobs_ahead,
            "slurm_pending_jobs_in_partition": state.pending_jobs_in_partition,
        }
        for key, value in mapping.items():
            if value is None:
                continue
            details[key] = value

    def _build_status_details(
        self,
        *,
        slurm_job_id: str | None,
        command_mode: str,
        datasets_synced: list[str],
        datasets_skipped: list[str],
        image: str | None,
        state: SlurmState | None = None,
        slurm_state: str | None = None,
        connectivity: str | None = None,
        unknown_since: float | None = None,
        stop_reason: str | None = None,
        error: str | None = None,
        executor_stage: str | None = None,
    ) -> dict[str, Any]:
        details: dict[str, Any] = {
            "executor": "deucalion",
            "command_mode": command_mode,
            "datasets_synced": datasets_synced,
            "datasets_skipped": datasets_skipped,
        }
        if slurm_job_id:
            details["slurm_job_id"] = slurm_job_id
        if image:
            details["image"] = image
        if state is not None:
            details["slurm_state"] = state.state
            self._inject_slurm_queue_details(details, state)
        elif slurm_state:
            details["slurm_state"] = slurm_state
        if connectivity:
            details["connectivity"] = connectivity
        if unknown_since is not None:
            details["unknown_since"] = unknown_since
        if stop_reason:
            details["backend_stop_reason"] = stop_reason
        if error:
            details["error"] = error
        if executor_stage:
            details["executor_stage"] = executor_stage
        return details

    def run_job(self, job: Dict[str, Any]) -> None:
        job_id = job["job_id"]
        config_path = str(job["config_path"]).lstrip("/")
        job_name = str(job.get("job_name", job_id))
        command = str(job.get("command") or self.runtime._build_command(job_id, config_path))
        image_name = str(job.get("image") or "")

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
        preflight_stage = "preflight:init"
        preflight_last_update = self.now_fn()
        preflight_last_probe = preflight_last_update

        try:
            self.runtime._register_active_job(job_id, job_name)
            self.runtime._update_active_job(job_id, phase="preflight:init", status="dispatched")
            self._append_local_log(local_log_path, f"Job accepted by deucalion worker: {job_id}")
            job_yaml = self._load_job_yaml(local_config_path)
            self._append_local_log(local_log_path, f"Loaded config: {config_path}")
            runtime_options = job.get("deucalion_options")
            cfg = resolve_deucalion_job_config(job_yaml, env=self.env, runtime_options=runtime_options)
            cfg = self._apply_job_image(cfg, job)
            image_name = cfg.sif_image
            remote_root = self._remote_root(cfg)
            command_mode = cfg.command_mode

            remote_job_dir = posixpath.join(remote_root, "runs", job_id)
            remote_data_dir = posixpath.join(remote_job_dir, "data")
            remote_cfg_path = posixpath.join(remote_data_dir, config_path)
            remote_script_path = posixpath.join(remote_job_dir, "run.sbatch")

            # Preflight
            preflight_stage = "preflight:remote_root_check"
            self._append_local_log(local_log_path, f"Preflight stage {preflight_stage} (remote_root={remote_root})")
            preflight_last_probe = self._maybe_interrupt_preflight(
                job_id=job_id,
                stage=preflight_stage,
                last_probe_at=preflight_last_probe,
            )
            self._check_remote_path(remote_root, kind="d")
            preflight_stage = "preflight:sif"
            self._append_local_log(local_log_path, "Preflight stage preflight:sif (ensure SIF image)")
            preflight_last_probe = self._maybe_interrupt_preflight(
                job_id=job_id,
                stage=preflight_stage,
                last_probe_at=preflight_last_probe,
            )
            self._ensure_remote_sif(cfg, job_id=job_id, local_log_path=local_log_path)
            for path in cfg.required_paths:
                preflight_stage = "preflight:required_paths"
                preflight_last_probe = self._maybe_interrupt_preflight(
                    job_id=job_id,
                    stage=preflight_stage,
                    last_probe_at=preflight_last_probe,
                )
                self._check_remote_path(path, kind="e")

            preflight_stage = "preflight:datasets"
            self._append_local_log(local_log_path, "Preflight stage preflight:datasets (sync datasets)")
            preflight_last_probe = self._maybe_interrupt_preflight(
                job_id=job_id,
                stage=preflight_stage,
                last_probe_at=preflight_last_probe,
            )
            datasets_synced, datasets_skipped = self._sync_datasets(cfg)
            self._append_local_log(
                local_log_path,
                f"Dataset sync complete (copied={len(datasets_synced)}, reused={len(datasets_skipped)})",
            )

            preflight_stage = "preflight:submit_setup"
            preflight_last_probe = self._maybe_interrupt_preflight(
                job_id=job_id,
                stage=preflight_stage,
                last_probe_at=preflight_last_probe,
            )
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

            now = self.now_fn()
            if (now - preflight_last_update) >= self.preflight_status_update_interval:
                self.runtime._post_status(
                    job_id,
                    "dispatched",
                    details=self._build_status_details(
                        slurm_job_id=None,
                        command_mode=command_mode,
                        datasets_synced=datasets_synced,
                        datasets_skipped=datasets_skipped,
                        image=image_name,
                        slurm_state="PREPARING",
                        executor_stage=preflight_stage,
                    ),
                    container_name=job_name,
                )
                preflight_last_update = now

            slurm_job_id = sbatch_submit(self.ssh, remote_script_path=remote_script_path, remote_workdir=remote_job_dir)
            self._append_local_log(local_log_path, f"Submitted Slurm job: {slurm_job_id}")
            initial_state = SlurmState(state="PENDING")
            try:
                initial_state = query_state(self.ssh, slurm_job_id)
            except SSHCommandError as exc:
                _LOGGER.warning("Unable to fetch initial Slurm queue details for job %s: %s", job_id, exc)
            details = self._build_status_details(
                slurm_job_id=slurm_job_id,
                command_mode=command_mode,
                datasets_synced=datasets_synced,
                datasets_skipped=datasets_skipped,
                image=image_name,
                state=initial_state,
                executor_stage="execution:dispatched",
            )
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

                    details = self._build_status_details(
                        slurm_job_id=slurm_job_id,
                        command_mode=command_mode,
                        datasets_synced=datasets_synced,
                        datasets_skipped=datasets_skipped,
                        image=image_name,
                        state=state,
                        stop_reason=stop_reason,
                        executor_stage="execution:poll",
                    )

                    if now >= next_sync:
                        log_offsets = self._sync_remote_logs(remote_job_dir, local_log_path, log_offsets)
                        self._sync_remote_progress_snapshot(
                            remote_job_dir=remote_job_dir,
                            remote_data_dir=remote_data_dir,
                            job_id=job_id,
                            local_job_dir=local_job_dir,
                        )
                        self._sync_remote_job_info_snapshot(
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
                        self._append_local_log(
                            local_log_path,
                            f"Slurm state unknown; keeping status '{report_status}' (unknown_since={unknown_since})",
                        )
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
                        self._append_local_log(
                            local_log_path,
                            f"Slurm state={state.state}; reported status='{report_status}'",
                        )
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
                    self._append_local_log(
                        local_log_path,
                        f"Terminal Slurm state={state.state}; reporting job status='{final_status}'",
                    )
                    self.runtime._post_status(job_id, final_status, exit_code=exit_code, error=error, details=details)
                    return
                except SSHCommandError as exc:
                    if degraded_since is None:
                        degraded_since = now
                    elapsed = now - degraded_since
                    details = self._build_status_details(
                        slurm_job_id=slurm_job_id,
                        command_mode=command_mode,
                        datasets_synced=datasets_synced,
                        datasets_skipped=datasets_skipped,
                        image=image_name,
                        slurm_state="UNKNOWN",
                        connectivity="degraded",
                        unknown_since=unknown_since,
                        stop_reason=stop_reason,
                        error=exc.stderr or str(exc),
                        executor_stage="execution:connectivity_degraded",
                    )
                    if stop_reason not in _BACKEND_OVERRIDE_NO_POST:
                        self.runtime._post_status(job_id, last_status, details=details)
                    self._append_local_log(local_log_path, f"Connectivity degraded while polling Slurm: {exc}")
                    if elapsed > self.unreachable_grace_seconds:
                        if stop_reason not in _BACKEND_OVERRIDE_NO_POST:
                            self.runtime._post_status(
                                job_id,
                                "failed",
                                error="deucalion_unreachable_timeout",
                                details=self._build_status_details(
                                    slurm_job_id=slurm_job_id,
                                    command_mode=command_mode,
                                    datasets_synced=datasets_synced,
                                    datasets_skipped=datasets_skipped,
                                    image=image_name,
                                    slurm_state="UNKNOWN",
                                    connectivity="down",
                                    unknown_since=unknown_since,
                                    stop_reason=stop_reason,
                                    executor_stage="execution:connectivity_down",
                                ),
                            )
                        self._append_local_log(
                            local_log_path,
                            "Connectivity timeout reached; marking job failed (deucalion_unreachable_timeout)",
                        )
                        return
                    self.sleep_fn(self.poll_interval)
                except PreflightInterrupted as exc:
                    stop_reason = exc.status
                    details = self._build_status_details(
                        slurm_job_id=slurm_job_id,
                        command_mode=command_mode,
                        datasets_synced=datasets_synced,
                        datasets_skipped=datasets_skipped,
                        image=image_name,
                        slurm_state="PREPARING",
                        stop_reason=stop_reason,
                        error=str(exc),
                        executor_stage=exc.stage,
                    )
                    resolved_status = self._resolve_interrupted_preflight_status(stop_reason)
                    self._append_local_log(local_log_path, str(exc))
                    if resolved_status:
                        self.runtime._post_status(job_id, resolved_status, details=details)
                    elif stop_reason not in _BACKEND_OVERRIDE_NO_POST:
                        self.runtime._post_status(job_id, "failed", error=str(exc), details=details)
                    return
                except Exception as exc:
                    _LOGGER.exception("Unexpected Deucalion execution failure for job %s", job_id)
                    self._append_local_log(local_log_path, f"Unexpected execution failure: {exc}")
                    self.runtime._post_status(
                        job_id,
                        "failed",
                        error=str(exc),
                        details=self._build_status_details(
                            slurm_job_id=slurm_job_id,
                            command_mode=command_mode,
                            datasets_synced=datasets_synced,
                            datasets_skipped=datasets_skipped,
                            image=image_name,
                            executor_stage="execution:unexpected_error",
                        ),
                    )
                    return
        except PreflightInterrupted as exc:
            stop_reason = exc.status
            details = self._build_status_details(
                slurm_job_id=slurm_job_id,
                command_mode=command_mode,
                datasets_synced=datasets_synced,
                datasets_skipped=datasets_skipped,
                image=image_name,
                slurm_state="PREPARING",
                stop_reason=stop_reason,
                error=str(exc),
                executor_stage=exc.stage,
            )
            resolved_status = self._resolve_interrupted_preflight_status(stop_reason)
            self._append_local_log(local_log_path, str(exc))
            if resolved_status:
                self.runtime._post_status(job_id, resolved_status, details=details)
            elif stop_reason not in _BACKEND_OVERRIDE_NO_POST:
                self.runtime._post_status(job_id, "failed", error=str(exc), details=details)
        except Exception as exc:
            _LOGGER.exception("Failed to submit Deucalion job %s", job_id)
            traceback_text = traceback.format_exc()
            self._append_local_log(local_log_path, f"Submission/preflight failure during {preflight_stage}: {exc}")
            self._append_local_log(local_log_path, traceback_text)
            self.runtime._post_status(
                job_id,
                "failed",
                error=str(exc),
                details=self._build_status_details(
                    slurm_job_id=slurm_job_id,
                    command_mode=command_mode,
                    datasets_synced=datasets_synced,
                    datasets_skipped=datasets_skipped,
                    image=image_name,
                    slurm_state="PREPARING",
                    executor_stage=preflight_stage,
                ),
            )
        finally:
            self.runtime._unregister_active_job(job_id)
            self.runtime._send_heartbeat(force=True)
