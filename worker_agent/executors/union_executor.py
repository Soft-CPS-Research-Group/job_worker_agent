from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
from typing import Any, Callable, Dict, Mapping

from worker_agent.union.archive import (
    append_missing_algorithm_logs,
    create_input_archive,
    merge_job_results,
    safe_extract,
)
from worker_agent.union.client import (
    FlyteUnionClient,
    UnionRunSnapshot,
    derive_object_uris,
    deterministic_run_name,
)
from worker_agent.union.config import UnionConfig
from worker_agent.union.events import parse_event

from .base import BaseExecutor, StaleJobAttemptError, WorkerRuntime


_LOGGER = logging.getLogger(__name__)
_TERMINAL_BACKEND_STATUSES = {"finished", "failed", "stopped", "canceled"}


class _RecoveryInterrupted(RuntimeError):
    pass


class UnionExecutor(BaseExecutor):
    def __init__(
        self,
        runtime: WorkerRuntime,
        *,
        env: Mapping[str, str] | None = None,
        client_factory: Callable[[UnionConfig], Any] | None = None,
        now_fn: Callable[[], float] = time.time,
        wait_fn: Callable[[float], bool] | None = None,
    ) -> None:
        self.runtime = runtime
        self.env = dict(env or os.environ)
        self.config = UnionConfig.from_env(self.env)
        self.client = (client_factory or FlyteUnionClient)(self.config)
        self.now_fn = now_fn
        self._state_lock = threading.RLock()
        self._recovery_threads: dict[str, threading.Thread] = {}
        self._claimed_jobs: set[str] = set()
        self._closing = threading.Event()
        self.wait_fn = wait_fn or self._closing.wait

    def heartbeat_info(self) -> Dict[str, Any]:
        info = {
            "gpu_enabled": True,
            "gpu_required": True,
            "union_endpoint": self.config.endpoint,
            "union_project": self.config.project,
            "union_domain": self.config.domain,
            "union_gpu_count": self.config.gpu_count,
            "union_auth_mode": self.config.auth_mode,
        }
        if self.config.auth_mode == "device_flow":
            info["union_auth"] = self.client.auth_state()
        return info

    def ready_for_new_jobs(self) -> bool:
        if self.config.auth_mode != "device_flow":
            return True
        return self.client.auth_state().get("status") == "authenticated"

    def handle_command(self, command: Dict[str, Any]) -> None:
        if command.get("action") == "union_authenticate":
            self.client.start_device_authentication(str(command.get("request_id") or ""))

    def _job_dir(self, job_id: str) -> Path:
        return Path(self.runtime.shared_dir) / "jobs" / job_id

    def _state_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / ".worker" / "union.json"

    def _load_state(self, job_id: str) -> dict[str, Any] | None:
        path = self._state_path(job_id)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _save_state(self, state: dict[str, Any]) -> None:
        job_id = str(state["job_id"])
        path = self._state_path(job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name("union.json.tmp")
        with self._state_lock:
            state["updated_at"] = self.now_fn()
            serialized = json.dumps(dict(state), indent=2, sort_keys=True)
            temporary.write_text(serialized, encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)

    def _claim_job(self, job_id: str) -> bool:
        with self._state_lock:
            if job_id in self._claimed_jobs:
                return False
            self._claimed_jobs.add(job_id)
            return True

    def _release_job(self, job_id: str) -> None:
        with self._state_lock:
            self._claimed_jobs.discard(job_id)

    def _abort_revoked_run(self, state: dict[str, Any]) -> bool:
        run_name = str(state.get("run_name") or "").strip()
        aborted = not bool(state.get("submitted"))
        if run_name and state.get("submitted"):
            try:
                self.client.abort(run_name, reason="OPEVA execution attempt was superseded")
                aborted = True
            except Exception as exc:  # pragma: no cover - depends on Union availability
                _LOGGER.warning("Unable to abort revoked Union run %s: %s", run_name, exc)
                state["revocation_error"] = str(exc)
        state["revoked"] = True
        state["revoked_at"] = self.now_fn()
        if aborted:
            state["terminal"] = True
            state["terminal_status"] = "canceled"
            state["terminal_stage"] = "union:revoked"
            state["orchestrator_ack"] = True
            state.pop("revocation_error", None)
        self._save_state(state)
        return aborted

    def _active_status(self, state: dict[str, Any]) -> str:
        return "running" if state.get("started_at") else "setup"

    def _active_details(self, state: dict[str, Any], *, stage: str, **extra: Any) -> dict[str, Any]:
        return {
            "executor_stage": stage,
            "started_at": state.get("started_at"),
            "union_run_id": state.get("run_name"),
            "union_run_url": state.get("run_url"),
            "union_phase": state.get("union_phase"),
            **extra,
        }

    @staticmethod
    def _is_retryable_control_plane_error(exc: Exception) -> bool:
        if isinstance(exc, (ValueError, TypeError, KeyError, FileNotFoundError)):
            return False
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if isinstance(status_code, int) and 400 <= status_code < 500:
            return status_code in {408, 409, 425, 429}
        message = str(exc).lower()
        permanent_markers = (
            "permission denied",
            "unauthenticated",
            "invalid argument",
            "invalid api key",
            "authentication failed",
            "not found",
        )
        return not any(marker in message for marker in permanent_markers)

    def _retry_control_plane(
        self,
        state: dict[str, Any],
        stage: str,
        operation: Callable[[], Any],
        *,
        keep_retrying_after_grace: bool = True,
    ) -> Any:
        delay = max(1, int(self.config.poll_interval_seconds))
        unreachable_since = state.get("control_plane_unreachable_since")
        if not isinstance(unreachable_since, (int, float)):
            unreachable_since = None

        while not self._closing.is_set():
            try:
                result = operation()
            except Exception as exc:
                if not self._is_retryable_control_plane_error(exc):
                    raise
                now = self.now_fn()
                if unreachable_since is None:
                    unreachable_since = now
                    self._append_log(
                        self.runtime._prepare_log_file(str(state["job_id"])),
                        f"[union-worker] Union control plane unavailable during {stage}: {exc}",
                    )
                elapsed = max(0.0, now - float(unreachable_since))
                connectivity = "down" if elapsed >= self.config.unreachable_grace_seconds else "degraded"
                if connectivity == "down" and not keep_retrying_after_grace:
                    raise
                state["control_plane_unreachable_since"] = unreachable_since
                state["control_plane_connectivity"] = connectivity
                state["last_control_plane_error"] = str(exc)
                state["last_control_plane_stage"] = stage

                last_report = state.get("last_control_plane_status_at")
                should_report = not isinstance(last_report, (int, float)) or (
                    now - float(last_report)
                ) >= self.config.status_update_interval_seconds
                if should_report:
                    self.runtime._post_status(
                        str(state["job_id"]),
                        self._active_status(state),
                        details=self._active_details(
                            state,
                            stage=f"union:{stage}:connectivity_{connectivity}",
                            connectivity=connectivity,
                            connectivity_error=str(exc),
                            connectivity_unavailable_seconds=int(elapsed),
                        ),
                    )
                    state["last_control_plane_status_at"] = now
                self._save_state(state)
                self.wait_fn(min(delay, self.config.retry_max_backoff_seconds))
                delay = min(delay * 2, self.config.retry_max_backoff_seconds)
                continue

            if unreachable_since is not None:
                self._append_log(
                    self.runtime._prepare_log_file(str(state["job_id"])),
                    f"[union-worker] Union control plane connectivity recovered during {stage}",
                )
                for key in (
                    "control_plane_unreachable_since",
                    "control_plane_connectivity",
                    "last_control_plane_error",
                    "last_control_plane_stage",
                    "last_control_plane_status_at",
                ):
                    state.pop(key, None)
                self._save_state(state)
            return result

        raise _RecoveryInterrupted(f"Union recovery interrupted during {stage}")

    @staticmethod
    def _append_log(path: Path, message: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(message.rstrip("\n") + "\n")
            handle.flush()

    def _write_progress(self, job_id: str, progress: dict[str, Any]) -> None:
        progress_dir = self._job_dir(job_id) / "progress"
        progress_dir.mkdir(parents=True, exist_ok=True)
        target = progress_dir / "progress.json"
        temporary = progress_dir / ".progress.json.union.tmp"
        payload = dict(progress)
        payload.setdefault("updated_at", self.now_fn())
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.chmod(temporary, 0o666)
        os.replace(temporary, target)
        try:
            os.chmod(progress_dir, 0o777)
            os.chmod(target, 0o666)
        except OSError:
            pass

    def on_startup(self) -> None:
        if self.config.auth_mode == "device_flow":
            self.client.start_device_authentication()
        jobs_root = Path(self.runtime.shared_dir) / "jobs"
        if not jobs_root.is_dir():
            return
        for state_path in jobs_root.glob("*/.worker/union.json"):
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(state, dict) or not state.get("run_name"):
                continue
            if state.get("terminal") is True and state.get("orchestrator_ack") is True:
                continue
            job_id = str(state.get("job_id") or state_path.parents[1].name)
            if self.runtime._fetch_status(job_id) == "queued":
                _LOGGER.info(
                    "Deferring Union recovery for queued job %s until the orchestrator dispatches it",
                    job_id,
                )
                continue
            if not self._claim_job(job_id):
                continue
            job_name = str(state.get("job_name") or job_id)
            job_payload = state.get("job_payload")
            if isinstance(job_payload, dict):
                self.runtime._bind_job_attempt(job_payload)
            self.runtime._register_active_job(job_id, job_name)
            self.runtime._update_active_job(job_id, phase="union:recovering", status="setup")
            thread = threading.Thread(
                target=self._recover,
                args=(state,),
                name=f"union-recover-{job_id[:8]}",
                daemon=True,
            )
            self._recovery_threads[job_id] = thread
            thread.start()
            _LOGGER.info("Recovering Union run %s for job %s", state.get("run_name"), job_id)

    def _recover(self, state: dict[str, Any]) -> None:
        job_id = str(state["job_id"])
        try:
            self._resume_state(state)
        except _RecoveryInterrupted:
            _LOGGER.info("Union recovery for job %s stopped with the worker", job_id)
        except StaleJobAttemptError:
            _LOGGER.warning("Recovered Union execution attempt for job %s was revoked", job_id)
            self._abort_revoked_run(state)
        except Exception as exc:
            _LOGGER.exception("Failed to recover Union job %s: %s", job_id, exc)
            self._finalize_terminal(state, "failed", error=str(exc), stage="union:recovery")
        finally:
            self.runtime._unregister_active_job(job_id)
            self.runtime._send_heartbeat(force=True)
            self._release_job(job_id)
            self._recovery_threads.pop(job_id, None)

    def _ensure_input_uploaded(self, state: dict[str, Any]) -> None:
        if state.get("input_uri"):
            return
        job = state.get("job_payload")
        if not isinstance(job, dict):
            raise RuntimeError("Union recovery state is missing the original job payload")
        job_id = str(state["job_id"])
        attempt = max(1, int(state.get("attempt") or 1))
        work_dir = self._job_dir(job_id) / ".worker"
        work_dir.mkdir(parents=True, exist_ok=True)
        archive_path = work_dir / f"union-input-a{attempt}.tar.gz"
        try:
            archived = create_input_archive(
                Path(self.runtime.shared_dir),
                job_id,
                str(job["config_path"]),
                archive_path,
            )
            self._append_log(
                self.runtime._prepare_log_file(job_id),
                f"[union-worker] Input package ready ({len(archived)} roots)",
            )
            self.runtime._post_status(
                job_id,
                "setup",
                details={"executor_stage": "union:uploading_input", "image": job.get("image")},
            )
            input_uri = self._retry_control_plane(
                state,
                "uploading_input",
                lambda: self.client.upload_input(archive_path, job_id, attempt),
            )
            result_uri, cancel_uri = derive_object_uris(str(input_uri))
            state["input_uri"] = str(input_uri)
            state["result_uri"] = result_uri
            state["cancel_uri"] = cancel_uri
            state["input_uploaded"] = True
            self._save_state(state)
        finally:
            archive_path.unlink(missing_ok=True)

    def _resume_state(self, state: dict[str, Any]) -> None:
        if state.get("terminal") is True:
            self._replay_terminal_status(state)
            return
        self._ensure_input_uploaded(state)
        if state.get("submitted") is not True:
            job = state.get("job_payload")
            if not isinstance(job, dict):
                raise RuntimeError("Union recovery state is missing the original job payload")
            self.runtime._post_status(
                str(state["job_id"]),
                "setup",
                details={
                    "executor_stage": "union:submitting",
                    "union_run_id": state.get("run_name"),
                    "image": job.get("image"),
                },
            )
            snapshot = self._retry_control_plane(
                state,
                "submitting",
                lambda: self.client.submit_job(
                    job=job,
                    run_name=str(state["run_name"]),
                    input_uri=str(state["input_uri"]),
                    result_uri=str(state["result_uri"]),
                    cancel_uri=str(state["cancel_uri"]),
                ),
            )
            state["submitted"] = True
            state["run_url"] = snapshot.url
            state["union_phase"] = snapshot.phase
            self._save_state(state)
            self._append_log(
                self.runtime._prepare_log_file(str(state["job_id"])),
                f"[union-worker] Submitted Union run {state['run_name']}",
            )
        self._monitor(state)

    def run_job(self, job: Dict[str, Any]) -> None:
        job_id = str(job["job_id"])
        job_name = str(job.get("job_name") or job_id)
        log_path = self.runtime._prepare_log_file(job_id)
        state: dict[str, Any] | None = None
        if not self._claim_job(job_id):
            self._append_log(
                log_path,
                "[union-worker] Existing recovery already owns this job; ignoring duplicate dispatch",
            )
            return
        try:
            self.runtime._register_active_job(job_id, job_name)
            self.runtime._update_active_job(job_id, phase="union:packaging", status="setup")
            self._append_log(log_path, f"[union-worker] Job accepted: {job_id}")
            self.runtime._post_status(
                job_id,
                "setup",
                details={"executor_stage": "union:packaging", "image": job.get("image")},
            )

            attempt = max(1, int(job.get("attempt_number") or 1))
            existing = self._load_state(job_id)
            existing_attempt = int(existing.get("attempt") or 1) if existing else None
            if existing and existing.get("run_name") and existing_attempt == attempt:
                state = existing
                self._append_log(log_path, f"[union-worker] Resuming existing run {state['run_name']}")
                self._resume_state(state)
                return
            if existing and existing.get("run_name") and existing_attempt != attempt:
                self._append_log(
                    log_path,
                    f"[union-worker] Superseding Union attempt {existing_attempt} with attempt {attempt}",
                )
                if existing.get("terminal") is not True and not self._abort_revoked_run(existing):
                    raise RuntimeError(
                        f"Unable to abort superseded Union run {existing['run_name']}; refusing overlapping attempts"
                    )

            run_name = deterministic_run_name(job_id, attempt)
            state = {
                "schema_version": 2,
                "job_id": job_id,
                "job_name": job_name,
                "worker_id": self.runtime.worker_id,
                "attempt": attempt,
                "run_name": run_name,
                "union_run_id": run_name,
                "image": str(job.get("image") or ""),
                "config_path": str(job["config_path"]),
                "created_at": self.now_fn(),
                "terminal": False,
                "orchestrator_ack": False,
                "last_event_sequence": 0,
                "log_line_count": 0,
                "algorithm_lines_relayed": 0,
                "submitted": False,
                "input_uploaded": False,
                "job_payload": dict(job),
            }
            self._save_state(state)
            self._resume_state(state)
        except _RecoveryInterrupted:
            _LOGGER.info("Union job %s stopped with the worker and remains recoverable", job_id)
        except StaleJobAttemptError:
            _LOGGER.warning("Union execution attempt for job %s was revoked", job_id)
            if state is not None:
                self._abort_revoked_run(state)
        except Exception as exc:
            _LOGGER.exception("Union job %s failed: %s", job_id, exc)
            self._append_log(log_path, f"[union-worker] Failure: {type(exc).__name__}: {exc}")
            if state is not None:
                self._finalize_terminal(state, "failed", error=str(exc), stage="union:failed")
            else:
                self.runtime._post_status(job_id, "failed", error=str(exc), details={"executor_stage": "union:failed"})
        finally:
            self.runtime._unregister_active_job(job_id)
            self.runtime._send_heartbeat(force=True)
            self._release_job(job_id)

    def _terminal_details(self, state: dict[str, Any], status: str, stage: str) -> dict[str, Any]:
        return {
            "executor_stage": stage,
            "union_run_id": state.get("run_name"),
            "union_run_url": state.get("run_url"),
            "union_phase": state.get("union_phase"),
            "terminal_status": status,
        }

    def _replay_terminal_status(self, state: dict[str, Any]) -> bool:
        status = str(state.get("terminal_status") or "failed")
        job_id = str(state["job_id"])
        backend_status = self.runtime._fetch_status(job_id)
        if backend_status in _TERMINAL_BACKEND_STATUSES:
            state["orchestrator_ack"] = True
            state["orchestrator_status"] = backend_status
            self._save_state(state)
            return True

        if status == "finished" and backend_status in {"dispatched", "setup"}:
            promoted = self.runtime._post_status(
                job_id,
                "running",
                details=self._active_details(state, stage="union:terminal_recovery"),
            )
            if not promoted:
                return False
        elif status == "stopped" and backend_status in {"dispatched", "setup"}:
            promoted = self.runtime._post_status(
                job_id,
                "stop_requested",
                details=self._active_details(state, stage="union:terminal_recovery"),
            )
            if not promoted:
                return False

        delivered = self.runtime._post_status(
            job_id,
            status,
            exit_code=state.get("runner_exit_code") if isinstance(state.get("runner_exit_code"), int) else None,
            error=state.get("error"),
            details=self._terminal_details(
                state,
                status,
                str(state.get("terminal_stage") or f"union:{status}"),
            ),
        )
        if delivered:
            state["orchestrator_ack"] = True
            state["orchestrator_status"] = status
            self._save_state(state)
            return True
        return False

    def _finalize_terminal(
        self,
        state: dict[str, Any],
        status: str,
        *,
        error: str | None = None,
        stage: str | None = None,
    ) -> None:
        state["terminal"] = True
        state["terminal_status"] = status
        state["terminal_stage"] = stage or f"union:{status}"
        state["orchestrator_ack"] = False
        if error:
            state["error"] = error
        self._save_state(state)
        self._replay_terminal_status(state)

    def _handle_log_line(self, state: dict[str, Any], log_path: Path, line: str) -> None:
        state["log_line_count"] = int(state.get("log_line_count", 0)) + 1
        event = parse_event(line)
        if event is None:
            marker = "[algorithms] "
            cleaned = line.strip("\n")
            if marker in cleaned:
                cleaned = cleaned.split(marker, 1)[1]
                state["algorithm_lines_relayed"] = int(state.get("algorithm_lines_relayed", 0)) + 1
            if cleaned:
                self._append_log(log_path, cleaned)
            if int(state["log_line_count"]) % 25 == 0:
                self._save_state(state)
            return

        sequence = int(event.get("sequence", 0))
        if sequence <= int(state.get("last_event_sequence", 0)):
            return
        state["last_event_sequence"] = sequence
        kind = str(event.get("kind"))
        job_id = str(state["job_id"])
        if kind == "setup":
            if isinstance(event.get("cancel_put_url"), str):
                state["cancel_put_url"] = event["cancel_put_url"]
            phase = str(event.get("phase") or "setup")
            state["runner_phase"] = phase
            self.runtime._update_active_job(job_id, phase=f"union:{phase}", status="setup")
        elif kind == "started":
            started_at = float(event.get("started_at") or event.get("timestamp") or self.now_fn())
            state["started_at"] = started_at
            self.runtime._post_status(
                job_id,
                "running",
                details={
                    "executor_stage": "union:running",
                    "started_at": started_at,
                    "union_run_id": state.get("run_name"),
                    "union_run_url": state.get("run_url"),
                },
            )
        elif kind == "progress" and isinstance(event.get("progress"), dict):
            self._write_progress(job_id, dict(event["progress"]))
        elif kind == "artifact" and isinstance(event.get("artifact"), dict):
            state["artifact"] = dict(event["artifact"])
        elif kind == "terminal":
            state["runner_terminal_status"] = str(event.get("status") or "failed")
            state["runner_exit_code"] = event.get("exit_code")
        self._save_state(state)

    def _start_log_pump(self, state: dict[str, Any], log_path: Path) -> tuple[threading.Thread, dict[str, Any]]:
        result: dict[str, Any] = {"error": None}
        skip_lines = int(state.get("log_line_count", 0))

        def _pump() -> None:
            try:
                for index, line in enumerate(self.client.stream_logs(str(state["run_name"]))):
                    if index < skip_lines:
                        continue
                    self._handle_log_line(state, log_path, str(line))
            except Exception as exc:  # remote logs are best effort; artifacts remain authoritative
                result["error"] = exc
                _LOGGER.warning("Union log stream ended for %s: %s", state["job_id"], exc)

        thread = threading.Thread(target=_pump, name=f"union-logs-{str(state['job_id'])[:8]}", daemon=True)
        thread.start()
        return thread, result

    def _monitor(self, state: dict[str, Any]) -> None:
        job_id = str(state["job_id"])
        log_path = self.runtime._prepare_log_file(job_id)
        run_name = str(state["run_name"])
        log_thread, log_result = self._start_log_pump(state, log_path)
        last_status_update = 0.0
        cancel_requested_at: float | None = None
        remote: UnionRunSnapshot | None = None

        while not self._closing.is_set():
            remote = self._retry_control_plane(
                state,
                "polling_run",
                lambda: self.client.get_run(run_name),
            )
            state["union_phase"] = remote.phase
            state["run_url"] = remote.url
            now = self.now_fn()
            backend_status = self.runtime._fetch_status(job_id)
            if backend_status in {"stop_requested", "canceled"}:
                if cancel_requested_at is None:
                    cancel_requested_at = now
                    put_url = str(state.get("cancel_put_url") or "")
                    if put_url and backend_status == "stop_requested":
                        try:
                            self.client.request_graceful_cancel(put_url)
                            self._append_log(log_path, "[union-worker] Graceful cancellation requested")
                        except Exception as exc:
                            _LOGGER.warning("Graceful Union cancellation failed: %s", exc)
                    else:
                        self._retry_control_plane(
                            state,
                            "aborting_run",
                            lambda: self.client.abort(run_name, reason=f"OPEVA job {backend_status}"),
                        )
                elif backend_status == "canceled" or (now - cancel_requested_at) >= self.config.graceful_stop_timeout_seconds:
                    self._retry_control_plane(
                        state,
                        "aborting_run",
                        lambda: self.client.abort(run_name, reason="OPEVA graceful cancellation timeout"),
                    )

            if now - last_status_update >= self.config.status_update_interval_seconds:
                status = "running" if state.get("started_at") else "setup"
                self.runtime._post_status(
                    job_id,
                    status,
                    details={
                        "executor_stage": "union:running" if status == "running" else "union:provisioning",
                        "started_at": state.get("started_at"),
                        "union_run_id": run_name,
                        "union_run_url": remote.url,
                        "union_phase": remote.phase,
                    },
                )
                last_status_update = now
                self._save_state(state)

            if remote.terminal:
                break
            if backend_status in _TERMINAL_BACKEND_STATUSES:
                break
            self._closing.wait(self.config.poll_interval_seconds)

        log_thread.join(timeout=15)
        if log_result.get("error"):
            self._append_log(log_path, f"[union-worker] Live logs unavailable: {log_result['error']}")
        if self._closing.is_set() and (remote is None or not remote.terminal):
            self._save_state(state)
            return
        if remote is None:
            raise RuntimeError(f"No Union state was returned for run {run_name}")

        backend_status = self.runtime._fetch_status(job_id)
        requested_stop = backend_status in {"stop_requested", "canceled", "stopped"}
        artifact = state.get("artifact") if isinstance(state.get("artifact"), dict) else None
        if artifact is None and state.get("result_uri"):
            try:
                artifact = self._retry_control_plane(
                    state,
                    "refreshing_artifact",
                    lambda: self.client.refresh_artifact(str(state["result_uri"]), job_id),
                    keep_retrying_after_grace=False,
                )
                state["artifact"] = artifact
                self._save_state(state)
            except Exception as exc:
                if not requested_stop:
                    raise RuntimeError(f"Union result artifact is unavailable: {exc}") from exc

        if artifact is not None:
            self._install_artifact(state, artifact, log_path)

        runner_status = str(state.get("runner_terminal_status") or "")
        success = (
            remote.phase.upper() == "SUCCEEDED"
            and runner_status in {"", "finished"}
            and state.get("artifact_installed") is True
            and state.get("artifact_deleted") is True
        )
        if requested_stop:
            final_status = "canceled" if backend_status == "canceled" else "stopped"
        else:
            final_status = "finished" if success else "failed"
        state["union_phase"] = remote.phase
        self._finalize_terminal(state, final_status)

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def _install_artifact(self, state: dict[str, Any], artifact: dict[str, Any], log_path: Path) -> None:
        job_id = str(state["job_id"])
        work_dir = self._job_dir(job_id) / ".worker"
        work_dir.mkdir(parents=True, exist_ok=True)
        archive_path = work_dir / "union-result.tar.gz"
        extracted: Path | None = None
        try:
            if state.get("artifact_installed") is not True:
                extracted = Path(tempfile.mkdtemp(prefix=f"opeva-union-{job_id[:8]}-", dir=str(work_dir)))

                def _download() -> None:
                    nonlocal artifact
                    try:
                        self.client.download_artifact(str(artifact["get_url"]), archive_path)
                    except Exception:
                        artifact = self.client.refresh_artifact(str(state["result_uri"]), job_id)
                        state["artifact"] = artifact
                        self._save_state(state)
                        self.client.download_artifact(str(artifact["get_url"]), archive_path)

                self._retry_control_plane(state, "downloading_artifact", _download)

                expected_size = int(artifact.get("size") or 0)
                expected_sha = str(artifact.get("sha256") or "")
                if expected_size <= 0 or archive_path.stat().st_size != expected_size:
                    raise RuntimeError(
                        f"Union artifact size mismatch: expected {expected_size}, got {archive_path.stat().st_size}"
                    )
                actual_sha = self._file_sha256(archive_path)
                if len(expected_sha) != 64 or actual_sha != expected_sha:
                    raise RuntimeError(f"Union artifact checksum mismatch: expected {expected_sha}, got {actual_sha}")
                safe_extract(archive_path, extracted)
                state["algorithm_lines_relayed"] = append_missing_algorithm_logs(
                    log_path,
                    extracted,
                    job_id,
                    int(state.get("algorithm_lines_relayed", 0)),
                )
                merge_job_results(extracted, Path(self.runtime.shared_dir), job_id)
                state["artifact_installed"] = True
                self._save_state(state)

            if state.get("artifact_deleted") is not True:
                def _delete() -> None:
                    nonlocal artifact
                    try:
                        self.client.delete_artifact(str(artifact["delete_url"]))
                    except Exception:
                        artifact = self.client.refresh_artifact(str(state["result_uri"]), job_id)
                        state["artifact"] = artifact
                        self._save_state(state)
                        self.client.delete_artifact(str(artifact["delete_url"]))

                self._retry_control_plane(state, "deleting_artifact", _delete)
                state["artifact_deleted"] = True
                self._save_state(state)
        finally:
            archive_path.unlink(missing_ok=True)
            if extracted is not None:
                shutil.rmtree(extracted, ignore_errors=True)

    def close(self) -> None:
        self._closing.set()
        for thread in list(self._recovery_threads.values()):
            thread.join(timeout=5)
