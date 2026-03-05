from __future__ import annotations

import logging
import os
import posixpath
import shlex
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

import yaml

from worker_agent.deucalion.config import DeucalionJobConfig, resolve_deucalion_job_config
from worker_agent.deucalion.slurm import ACTIVE_STATES, SlurmState, query_state, sbatch_submit, scancel_job
from worker_agent.deucalion.ssh_client import SSHClient, SSHCommandError, SSHSettings

from .base import BaseExecutor, WorkerRuntime

_LOGGER = logging.getLogger(__name__)


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

    def _sync_remote_logs(self, remote_job_dir: str, local_log_path: Path) -> None:
        output = self.ssh.run(
            f"cat {shlex.quote(posixpath.join(remote_job_dir, 'slurm.out'))} "
            f"{shlex.quote(posixpath.join(remote_job_dir, 'slurm.err'))} 2>/dev/null || true",
            timeout=60,
            check=False,
        )
        local_log_path.parent.mkdir(parents=True, exist_ok=True)
        local_log_path.write_text(output, encoding="utf-8")

    def _sync_remote_artifacts(self, remote_job_dir: str, local_job_dir: Path) -> None:
        local_job_dir.mkdir(parents=True, exist_ok=True)
        for folder in ("results", "progress"):
            remote_path = posixpath.join(remote_job_dir, folder)
            exists = self.ssh.run(f"test -d {shlex.quote(remote_path)} && echo yes || true", check=False)
            if exists.strip() != "yes":
                continue
            try:
                self.ssh.copy_from(remote_path, local_job_dir, recursive=True)
            except SSHCommandError as exc:
                _LOGGER.warning("Failed to sync remote '%s': %s", folder, exc)

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
        lines.append(
            "singularity exec "
            f"--bind {shlex.quote(remote_data_dir)}:/data "
            f"{shlex.quote(cfg.sif_path)} "
            f"{command}"
        )
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
        stop_reason: str | None = None
        last_status = "dispatched"

        try:
            self.runtime._mark_active_job(job_id)
            job_yaml = self._load_job_yaml(local_config_path)
            cfg = resolve_deucalion_job_config(job_yaml, env=self.env)
            remote_root = self._remote_root(cfg)

            remote_job_dir = posixpath.join(remote_root, "runs", job_id)
            remote_data_dir = posixpath.join(remote_job_dir, "data")
            remote_cfg_path = posixpath.join(remote_data_dir, config_path)
            remote_script_path = posixpath.join(remote_job_dir, "run.sbatch")

            # Preflight
            self._check_remote_path(remote_root, kind="d")
            self._check_remote_path(cfg.sif_path, kind="f")
            for path in cfg.required_paths:
                self._check_remote_path(path, kind="e")

            self.ssh.run(
                f"mkdir -p {shlex.quote(remote_job_dir)} "
                f"{shlex.quote(posixpath.dirname(remote_cfg_path))}",
                timeout=30,
            )
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
            details = {"slurm_job_id": slurm_job_id, "slurm_state": "PENDING", "executor": "deucalion"}
            self.runtime._post_status(job_id, "dispatched", details=details, container_name=job_name)
            last_status = "dispatched"

            next_sync = 0.0
            while True:
                now = self.now_fn()
                try:
                    if self.runtime.status_poll_interval > 0:
                        backend_status = self.runtime._fetch_status(job_id)
                        if backend_status in {"stop_requested", "canceled"}:
                            stop_reason = backend_status
                            if slurm_job_id:
                                scancel_job(self.ssh, slurm_job_id)

                    state = query_state(self.ssh, slurm_job_id)
                    degraded_since = None

                    details = {"slurm_job_id": slurm_job_id, "slurm_state": state.state, "executor": "deucalion"}

                    if now >= next_sync:
                        self._sync_remote_logs(remote_job_dir, local_log_path)
                        next_sync = now + self.sync_interval

                    if state.state in ACTIVE_STATES or state.state == "UNKNOWN":
                        running_states = {"RUNNING", "COMPLETING", "STAGE_OUT"}
                        report_status = "running" if state.state in running_states else "dispatched"
                        self.runtime._post_status(job_id, report_status, details=details)
                        last_status = report_status
                        self.sleep_fn(self.poll_interval)
                        continue

                    final_status, exit_code, error = self._map_terminal_status(stop_reason, state)
                    self._sync_remote_logs(remote_job_dir, local_log_path)
                    self._sync_remote_artifacts(remote_job_dir, local_job_dir)
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
                        "error": exc.stderr or str(exc),
                    }
                    self.runtime._post_status(job_id, last_status, details=details)
                    if elapsed > self.unreachable_grace_seconds:
                        self.runtime._post_status(
                            job_id,
                            "failed",
                            error="deucalion_unreachable_timeout",
                            details={"slurm_job_id": slurm_job_id, "connectivity": "down", "executor": "deucalion"},
                        )
                        return
                    self.sleep_fn(self.poll_interval)
                except Exception as exc:
                    _LOGGER.exception("Unexpected Deucalion execution failure for job %s", job_id)
                    self.runtime._post_status(
                        job_id,
                        "failed",
                        error=str(exc),
                        details={"slurm_job_id": slurm_job_id, "executor": "deucalion"},
                    )
                    return
        except Exception as exc:
            _LOGGER.exception("Failed to submit Deucalion job %s", job_id)
            self.runtime._post_status(
                job_id,
                "failed",
                error=str(exc),
                details={"executor": "deucalion"},
            )
        finally:
            self.runtime._send_heartbeat(force=True)
            self.runtime._mark_active_job(None)
