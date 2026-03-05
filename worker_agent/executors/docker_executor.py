from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

try:  # docker is optional at import time for tooling
    import docker
except ImportError:  # pragma: no cover - tests inject a fake client
    docker = None  # type: ignore

from .base import BaseExecutor, WorkerRuntime

_LOGGER = logging.getLogger(__name__)


class DockerExecutor(BaseExecutor):
    def __init__(
        self,
        runtime: WorkerRuntime,
        docker_client_factory: Optional[Callable[[], "docker.DockerClient"]] = None,
    ) -> None:
        self.runtime = runtime
        if docker_client_factory is None:
            if docker is None:
                raise RuntimeError("docker package is not available")
            docker_client_factory = lambda: docker.DockerClient(base_url="unix://var/run/docker.sock")
        self._docker_client_factory = docker_client_factory
        self._docker_client_instance: Optional["docker.DockerClient"] = None

    def _get_docker_client(self) -> "docker.DockerClient":
        if self._docker_client_instance is None:
            self._docker_client_instance = self._docker_client_factory()
        return self._docker_client_instance

    def run_job(self, job: Dict[str, Any]) -> None:
        job_id = job["job_id"]
        config_path = job["config_path"]
        job_name = job.get("job_name", job_id)

        container = None
        monitor_thread: Optional[threading.Thread] = None
        monitor_state = {"status": None}
        monitor_stop = threading.Event()
        try:
            self.runtime._mark_active_job(job_id)
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
            run_kwargs = {
                "image": job.get("image", self.runtime.image),
                "command": command,
                "name": container_name,
                "volumes": volumes,
                "environment": env,
                "labels": labels,
                "detach": True,
            }
            if device_requests:
                run_kwargs["device_requests"] = device_requests
            try:
                container = client.containers.run(**run_kwargs)
            except Exception as exc:
                if device_requests:
                    _LOGGER.info("GPU request failed (%s); retrying without GPU", exc)
                    run_kwargs.pop("device_requests", None)
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

                def _monitor() -> None:
                    while not monitor_stop.wait(self.runtime.status_poll_interval):
                        status = self.runtime._fetch_status(job_id)
                        if status in {"stop_requested", "canceled"}:
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
            else:
                status = "finished" if exit_code == 0 else "failed"
                self.runtime._post_status(job_id, status, exit_code=exit_code)
                _LOGGER.info("Job %s exited with status '%s' (exit code %s)", job_id, status, exit_code)
        except Exception as exc:
            _LOGGER.exception("Job %s failed: %s", job_id, exc)
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
            self.runtime._send_heartbeat(force=True)
            self.runtime._mark_active_job(None)

    def close(self) -> None:
        client = self._docker_client_instance
        if client is not None:
            try:
                client.close()
            except Exception:  # pragma: no cover
                pass
