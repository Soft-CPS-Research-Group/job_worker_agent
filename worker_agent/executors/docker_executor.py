from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional

try:  # docker is optional at import time for tooling
    import docker
except ImportError:  # pragma: no cover - tests inject a fake client
    docker = None  # type: ignore

from .base import BaseExecutor, WorkerRuntime

_LOGGER = logging.getLogger(__name__)
_RUNNING_CONTAINER_STATES = {"running", "restarting"}
_BACKEND_ACTIVE_JOB_STATUSES = {"launching", "dispatched", "running", "stop_requested"}
_BACKEND_TERMINAL_OR_QUEUED_STATUSES = {"queued", "finished", "failed", "stopped", "canceled"}


class DockerExecutor(BaseExecutor):
    def __init__(
        self,
        runtime: WorkerRuntime,
        docker_client_factory: Optional[Callable[[], "docker.DockerClient"]] = None,
    ) -> None:
        self.runtime = runtime
        self.pull_policy = os.environ.get("WORKER_DOCKER_PULL_POLICY", "always").strip().lower()
        if self.pull_policy not in {"always", "if-not-present", "never"}:
            self.pull_policy = "always"
        if docker_client_factory is None:
            if docker is None:
                raise RuntimeError("docker package is not available")
            docker_client_factory = lambda: docker.DockerClient(base_url="unix://var/run/docker.sock")
        self._docker_client_factory = docker_client_factory
        self._docker_client_instance: Optional["docker.DockerClient"] = None
        self._cleanup_enabled = os.environ.get("WORKER_DOCKER_ORPHAN_CLEANUP", "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._cleanup_interval_seconds = max(
            0.0,
            float(os.environ.get("WORKER_DOCKER_ORPHAN_CLEANUP_INTERVAL_SECONDS", "30")),
        )
        self._last_cleanup_ts = 0.0

    def _get_docker_client(self) -> "docker.DockerClient":
        if self._docker_client_instance is None:
            self._docker_client_instance = self._docker_client_factory()
        return self._docker_client_instance

    def _is_name_conflict_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return "conflict" in message and "already in use" in message

    def _remove_stale_container(self, client: "docker.DockerClient", container_name: str) -> bool:
        try:
            stale = client.containers.get(container_name)
        except Exception:
            return False
        try:
            stale.remove(force=True)
            _LOGGER.warning("Removed stale container '%s' before retry", container_name)
            return True
        except Exception as remove_exc:
            _LOGGER.warning("Failed to remove stale container '%s': %s", container_name, remove_exc)
            return False

    def _append_startup_error_log(self, job_id: str, exc: Exception) -> None:
        try:
            log_path = self.runtime._prepare_log_file(job_id)
            with open(log_path, "a", encoding="utf-8") as log_file:
                log_file.write(f"[worker] startup failure: {exc}\n")
        except Exception:  # pragma: no cover - defensive logging
            pass

    def _pull_image(self, client: "docker.DockerClient", image_ref: str) -> None:
        if self.pull_policy == "never":
            return
        images_api = getattr(client, "images", None)
        if images_api is None or not hasattr(images_api, "pull"):
            return

        if self.pull_policy == "if-not-present" and hasattr(images_api, "get"):
            try:
                images_api.get(image_ref)
                return
            except Exception:
                pass

        try:
            images_api.pull(image_ref)
            _LOGGER.info("Pulled image %s before run (policy=%s)", image_ref, self.pull_policy)
        except Exception as exc:  # pragma: no cover - depends on daemon/network
            _LOGGER.warning("Failed to pull image %s (policy=%s): %s", image_ref, self.pull_policy, exc)

    @staticmethod
    def _container_labels(container: Any) -> Dict[str, str]:
        labels = getattr(container, "labels", None)
        if isinstance(labels, dict):
            return {str(k): str(v) for k, v in labels.items()}
        attrs = getattr(container, "attrs", None)
        if isinstance(attrs, dict):
            config = attrs.get("Config")
            if isinstance(config, dict):
                attrs_labels = config.get("Labels")
                if isinstance(attrs_labels, dict):
                    return {str(k): str(v) for k, v in attrs_labels.items()}
        return {}

    @staticmethod
    def _container_status(container: Any) -> str:
        state = str(getattr(container, "status", "") or "").strip().lower()
        if state:
            return state
        attrs = getattr(container, "attrs", None)
        if isinstance(attrs, dict):
            state_payload = attrs.get("State")
            if isinstance(state_payload, dict):
                status = str(state_payload.get("Status") or "").strip().lower()
                if status:
                    return status
        return "unknown"

    def _list_job_labeled_containers(self, client: "docker.DockerClient") -> list[Any]:
        containers_api = getattr(client, "containers", None)
        if containers_api is None or not hasattr(containers_api, "list"):
            return []
        try:
            return list(containers_api.list(all=True, filters={"label": "opeva.job_id"}))
        except TypeError:
            return list(containers_api.list(all=True))
        except Exception as exc:  # pragma: no cover - daemon/runtime dependent
            _LOGGER.warning("Failed to list existing containers for orphan cleanup: %s", exc)
            return []

    def _cleanup_orphan_job_containers(self, client: "docker.DockerClient", *, force: bool = False) -> None:
        if not self._cleanup_enabled:
            return
        now = time.monotonic()
        if not force and self._cleanup_interval_seconds > 0 and (now - self._last_cleanup_ts) < self._cleanup_interval_seconds:
            return
        self._last_cleanup_ts = now

        for container in self._list_job_labeled_containers(client):
            labels = self._container_labels(container)
            job_id = str(labels.get("opeva.job_id", "")).strip()
            if not job_id:
                continue
            owner_worker = str(labels.get("opeva.worker_id", "")).strip()
            if owner_worker and owner_worker != self.runtime.worker_id:
                continue

            status = self._container_status(container)
            container_name = str(getattr(container, "name", "<unknown>"))
            if status not in _RUNNING_CONTAINER_STATES:
                try:
                    container.remove(force=True)
                    _LOGGER.info(
                        "Removed stale job container '%s' (job=%s, state=%s)",
                        container_name,
                        job_id,
                        status,
                    )
                except Exception as exc:  # pragma: no cover
                    _LOGGER.warning(
                        "Failed to remove stale job container '%s' (job=%s): %s",
                        container_name,
                        job_id,
                        exc,
                    )
                continue

            backend_status = self.runtime._fetch_status(job_id)
            if backend_status in _BACKEND_ACTIVE_JOB_STATUSES or backend_status is None:
                # Keep healthy/unknown running jobs to avoid unsafe interruption.
                continue
            if backend_status in _BACKEND_TERMINAL_OR_QUEUED_STATUSES:
                try:
                    if hasattr(container, "stop"):
                        container.stop()
                except Exception:
                    pass
                try:
                    container.remove(force=True)
                    _LOGGER.info(
                        "Removed orphan running container '%s' (job=%s, backend_status=%s)",
                        container_name,
                        job_id,
                        backend_status,
                    )
                except Exception as exc:  # pragma: no cover
                    _LOGGER.warning(
                        "Failed to remove orphan running container '%s' (job=%s): %s",
                        container_name,
                        job_id,
                        exc,
                    )

    def on_startup(self) -> None:
        client = self._get_docker_client()
        self._cleanup_orphan_job_containers(client, force=True)

    def run_job(self, job: Dict[str, Any]) -> None:
        job_id = job["job_id"]
        config_path = job["config_path"]
        job_name = job.get("job_name", job_id)

        container = None
        monitor_thread: Optional[threading.Thread] = None
        monitor_state = {"status": None}
        monitor_stop = threading.Event()
        try:
            self.runtime._register_active_job(job_id, str(job_name))
            self.runtime._update_active_job(job_id, phase="startup")
            container_name = job.get("container_name") or self.runtime._build_container_name(job_id, job_name)
            command = job.get("command") or self.runtime._build_command(job_id, config_path)
            volumes = self.runtime._build_volumes(job.get("volumes"))
            env = job.get("env", {}) or {}
            try:
                device_requests = self.runtime._build_device_requests(job)
            except TypeError:
                # Maintain compatibility with tests that monkeypatch without params
                device_requests = self.runtime._build_device_requests()
            labels = {
                "opeva.worker_id": self.runtime.worker_id,
                "opeva.job_id": job_id,
            }
            _LOGGER.info("Starting container %s for job %s", container_name, job_id)
            client = self._get_docker_client()
            self._cleanup_orphan_job_containers(client)
            run_kwargs = {
                "image": job.get("image", self.runtime.image),
                "command": command,
                "name": container_name,
                "volumes": volumes,
                "environment": env,
                "labels": labels,
                "detach": True,
            }
            self._pull_image(client, run_kwargs["image"])
            if device_requests:
                run_kwargs["device_requests"] = device_requests
            try:
                container = client.containers.run(**run_kwargs)
            except Exception as exc:
                if device_requests:
                    _LOGGER.info("GPU request failed (%s); retrying without GPU", exc)
                    self._remove_stale_container(client, container_name)
                    run_kwargs.pop("device_requests", None)
                    try:
                        container = client.containers.run(**run_kwargs)
                    except Exception as retry_exc:
                        if self._is_name_conflict_error(retry_exc):
                            self._remove_stale_container(client, container_name)
                            container = client.containers.run(**run_kwargs)
                        else:
                            raise
                elif self._is_name_conflict_error(exc):
                    _LOGGER.warning(
                        "Container name conflict for '%s'; removing stale container and retrying once",
                        container_name,
                    )
                    self._remove_stale_container(client, container_name)
                    container = client.containers.run(**run_kwargs)
                else:
                    raise
            container_id = getattr(container, "id", None)
            container_name = getattr(container, "name", None)
            self.runtime._post_status(
                job_id,
                "running",
                container_id=container_id,
                container_name=container_name,
            )

            if self.runtime.status_poll_interval > 0:
                backend_stop_states = {
                    "stop_requested",
                    "canceled",
                    "queued",
                    "failed",
                    "finished",
                    "stopped",
                }

                def _monitor() -> None:
                    while not monitor_stop.wait(self.runtime.status_poll_interval):
                        status = self.runtime._fetch_status(job_id)
                        if status in backend_stop_states:
                            monitor_state["status"] = status
                            try:
                                if hasattr(container, "stop"):
                                    container.stop()
                            except Exception:  # pragma: no cover
                                pass
                            break
                        self.runtime._post_status(
                            job_id,
                            "running",
                            container_id=container_id,
                            container_name=container_name,
                        )

                monitor_thread = threading.Thread(target=_monitor, name=f"monitor-{job_id}")
                monitor_thread.daemon = True
                monitor_thread.start()

            log_path = self.runtime._prepare_log_file(job_id)
            _LOGGER.info("Streaming logs for job %s into %s", job_id, log_path)
            with open(log_path, "a", encoding="utf-8") as log_file:
                for chunk in container.logs(stream=True, follow=True):
                    text = chunk.decode("utf-8", errors="replace") if isinstance(chunk, bytes) else str(chunk)
                    log_file.write(text)
                    log_file.flush()

            result = container.wait()
            exit_code = None
            if isinstance(result, dict):
                exit_code = result.get("StatusCode")
            final_status = monitor_state.get("status")
            if final_status in {"stop_requested", "canceled"}:
                reported_status = "stopped" if final_status == "stop_requested" else "canceled"
                self.runtime._post_status(job_id, reported_status, exit_code=exit_code)
                _LOGGER.info(
                    "Job %s completed with backend status '%s' (exit code %s)",
                    job_id,
                    reported_status,
                    exit_code,
                )
            elif final_status in {"queued", "failed", "finished", "stopped"}:
                _LOGGER.info(
                    "Job %s stopped locally because backend moved it to '%s' (exit code %s)",
                    job_id,
                    final_status,
                    exit_code,
                )
            else:
                status = "finished" if exit_code == 0 else "failed"
                self.runtime._post_status(job_id, status, exit_code=exit_code)
                _LOGGER.info("Job %s exited with status '%s' (exit code %s)", job_id, status, exit_code)
        except Exception as exc:
            _LOGGER.exception("Job %s failed: %s", job_id, exc)
            self._append_startup_error_log(job_id, exc)
            self.runtime._post_status(job_id, "failed", error=str(exc))
        finally:
            monitor_stop.set()
            if monitor_thread is not None:
                monitor_thread.join(timeout=1)
            if container is not None:
                try:
                    container.remove(force=True)
                except Exception:  # pragma: no cover
                    pass
            self.runtime._unregister_active_job(job_id)
            self.runtime._send_heartbeat(force=True)

    def close(self) -> None:
        client = self._docker_client_instance
        if client is not None:
            try:
                client.close()
            except Exception:  # pragma: no cover
                pass
