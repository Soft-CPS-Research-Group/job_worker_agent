from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable, Dict, Optional

import requests

try:  # docker is optional at import time for tooling
    import docker
    from docker.models.containers import Container
    try:
        from docker.types import DeviceRequest
    except ImportError:  # pragma: no cover - older docker SDK
        DeviceRequest = None  # type: ignore
except ImportError:  # pragma: no cover - tests inject a fake client
    docker = None  # type: ignore
    Container = object  # type: ignore
    DeviceRequest = None  # type: ignore


_LOGGER = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class WorkerAgent:
    """Agent that polls the OPEVA backend for work and executes Docker jobs."""

    def __init__(
        self,
        server_url: str,
        worker_id: str,
        shared_dir: str,
        image: str,
        poll_interval: float = 5.0,
        heartbeat_interval: float = 30.0,
        status_poll_interval: float = 10.0,
        exit_after_job: bool = False,
        session: Optional[requests.Session] = None,
        docker_client_factory: Optional[Callable[[], "docker.DockerClient"]] = None,
    ) -> None:
        if docker_client_factory is None:
            if docker is None:
                raise RuntimeError("docker package is not available")
            docker_client_factory = lambda: docker.DockerClient(base_url="unix://var/run/docker.sock")

        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id
        self.shared_dir = shared_dir
        self.image = image
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.status_poll_interval = status_poll_interval
        self._exit_after_job = exit_after_job
        self._last_heartbeat = 0.0
        self._stop_event = threading.Event()
        self._external_session = session is not None
        self._session = session or requests.Session()
        self._docker_client_factory = docker_client_factory
        self._docker_client_instance: Optional["docker.DockerClient"] = None
        self._active_job_id: Optional[str] = None
        self._gpu_request_enabled = DeviceRequest is not None
        self._last_request_failure: Optional[str] = None

    # ------------------------------------------------------------------
    # Lifecycle helpers
    def stop(self) -> None:
        self._stop_event.set()

    def run_forever(self) -> None:
        _LOGGER.info("Starting worker '%s' polling %s", self.worker_id, self.server_url)
        try:
            while not self._stop_event.is_set():
                handled = self.poll_once()
                sleep_for = 0 if handled else self.poll_interval
                if sleep_for > 0:
                    time.sleep(sleep_for)
        except KeyboardInterrupt:
            _LOGGER.info("Worker interrupted, shutting down")
        finally:
            client = self._docker_client_instance
            if client is not None:
                try:
                    client.close()
                except Exception:  # pragma: no cover
                    pass

    def poll_once(self) -> bool:
        self._send_heartbeat()
        job = self._request_next_job()
        if not job:
            return False
        _LOGGER.info("Received job %s", job["job_id"])
        self._run_job(job)
        if self._exit_after_job:
            _LOGGER.info("Exit-after-job flag set; stopping worker once current job completes")
            self.stop()
        return True

    # ------------------------------------------------------------------
    # HTTP interactions
    def _send_heartbeat(self, force: bool = False) -> None:
        now = time.time()
        if not force:
            if self.heartbeat_interval > 0 and (now - self._last_heartbeat) < self.heartbeat_interval:
                return
        payload = {"worker_id": self.worker_id}
        try:
            self._session.post(
                f"{self.server_url}/api/agent/heartbeat",
                json=payload,
                timeout=10,
            )
            self._last_heartbeat = now
            self._last_request_failure = None
        except requests.RequestException as exc:  # pragma: no cover - network issues
            self._handle_request_exception("heartbeat", exc, warning=True)

    def _request_next_job(self) -> Optional[Dict[str, str]]:
        try:
            response = self._session.post(
                f"{self.server_url}/api/agent/next-job",
                json={"worker_id": self.worker_id},
                timeout=30,
            )
            self._last_request_failure = None
        except requests.RequestException as exc:  # pragma: no cover
            self._handle_request_exception("next-job", exc, warning=True)
            return None

        if response.status_code == 204:
            return None
        response.raise_for_status()
        return response.json()

    def _post_status(self, job_id: str, status: str, **extra: object) -> None:
        payload = {"job_id": job_id, "status": status, "worker_id": self.worker_id}
        payload.update({k: v for k, v in extra.items() if v is not None})
        try:
            self._session.post(
                f"{self.server_url}/api/agent/job-status",
                json=payload,
                timeout=10,
            )
            self._last_request_failure = None
        except requests.RequestException as exc:  # pragma: no cover
            self._handle_request_exception(f"job-status({job_id})", exc, warning=True)

    def _fetch_status(self, job_id: str) -> Optional[str]:
        try:
            response = self._session.get(
                f"{self.server_url}/status/{job_id}", timeout=10
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            payload = response.json()
            return payload.get("status")
        except requests.RequestException as exc:  # pragma: no cover
            _LOGGER.warning("Failed to fetch status for %s: %s", job_id, exc)
            return None

    # ------------------------------------------------------------------
    # Docker handling
    def _get_docker_client(self) -> "docker.DockerClient":
        if self._docker_client_instance is None:
            self._docker_client_instance = self._docker_client_factory()
        return self._docker_client_instance

    def _run_job(self, job: Dict[str, str]) -> None:
        job_id = job["job_id"]
        config_path = job["config_path"]
        job_name = job.get("job_name", job_id)

        container = None
        monitor_thread: Optional[threading.Thread] = None
        monitor_state = {"status": None}
        monitor_stop = threading.Event()
        try:
            self._active_job_id = job_id
            container_name = self._build_container_name(job_id, job_name)
            command = self._build_command(job_id, config_path)
            volumes = {self.shared_dir: {"bind": "/data", "mode": "rw"}}
            device_requests = self._build_device_requests()
            labels = {
                "opeva.worker_id": self.worker_id,
                "opeva.job_id": job_id,
            }
            _LOGGER.info("Starting container %s for job %s", container_name, job_id)
            client = self._get_docker_client()
            run_kwargs = {
                "image": self.image,
                "command": command,
                "name": container_name,
                "volumes": volumes,
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
            self._post_status(
                job_id,
                "running",
                container_id=getattr(container, "id", None),
                container_name=getattr(container, "name", None),
            )

            if self.status_poll_interval > 0:

                def _monitor() -> None:
                    while not monitor_stop.wait(self.status_poll_interval):
                        status = self._fetch_status(job_id)
                        if status in {"stopped", "canceled"}:
                            monitor_state["status"] = status
                            try:
                                if hasattr(container, "stop"):
                                    container.stop()
                            except Exception:  # pragma: no cover
                                pass
                            break

                monitor_thread = threading.Thread(target=_monitor, name=f"monitor-{job_id}")
                monitor_thread.daemon = True
                monitor_thread.start()

            log_path = self._prepare_log_file(job_id)
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
            if final_status in {"stopped", "canceled"}:
                self._post_status(job_id, final_status, exit_code=exit_code)
                _LOGGER.info("Job %s completed with backend status '%s' (exit code %s)", job_id, final_status, exit_code)
            else:
                status = "finished" if exit_code == 0 else "failed"
                self._post_status(job_id, status, exit_code=exit_code)
                _LOGGER.info("Job %s exited with status '%s' (exit code %s)", job_id, status, exit_code)
        except Exception as exc:
            _LOGGER.exception("Job %s failed: %s", job_id, exc)
            self._post_status(job_id, "failed", error=str(exc))
        finally:
            monitor_stop.set()
            if monitor_thread is not None:
                monitor_thread.join(timeout=1)
            if container is not None:
                try:
                    container.remove(force=True)
                except Exception:  # pragma: no cover
                    pass
            self._send_heartbeat(force=True)
            self._active_job_id = None

    # ------------------------------------------------------------------
    # Helpers
    def _build_command(self, job_id: str, config_path: str) -> str:
        return f"--config /data/{config_path} --job_id {job_id}"

    def _build_device_requests(self) -> Optional[list]:
        if not self._gpu_request_enabled:
            return None
        try:
            return [DeviceRequest(count=-1, capabilities=[["gpu"]])]
        except Exception:  # pragma: no cover - defensive
            return None

    def _reset_session(self) -> None:
        if self._external_session:
            return
        try:
            self._session.close()
        except Exception:
            pass
        self._session = requests.Session()

    def _handle_request_exception(self, context: str, exc: requests.RequestException, warning: bool = False) -> None:
        is_repeat = self._last_request_failure == context
        log_func = _LOGGER.warning if warning and not is_repeat else (_LOGGER.debug if is_repeat else _LOGGER.error)
        log_func("Request to %s failed: %s", context, exc)
        self._reset_session()
        _LOGGER.debug("HTTP session reset after %s failure", context)
        self._last_request_failure = context

    def _build_container_name(self, job_id: str, job_name: str) -> str:
        safe_job = job_name.replace(" ", "_")
        return f"job_{self.worker_id}_{safe_job}_{job_id[:8]}"

    def _prepare_log_file(self, job_id: str) -> Path:
        logs_dir = Path(self.shared_dir) / "jobs" / job_id / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return logs_dir / f"{job_id}.log"

    def request_exit_after_current_job(self) -> None:
        """Ensure the worker stops after the currently running job."""
        self._exit_after_job = True
        if self._active_job_id is None:
            _LOGGER.info("Exit-after-job requested while idle; stopping worker immediately")
            self.stop()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a job worker agent")
    parser.add_argument("--server", default=os.environ.get("OPEVA_SERVER", "http://localhost:8000"))
    parser.add_argument("--worker-id", default=os.environ.get("WORKER_ID"))
    parser.add_argument("--shared-dir", default=os.environ.get("OPEVA_SHARED_DIR", "/opt/opeva_shared_data"))
    parser.add_argument("--image", default=os.environ.get("WORKER_IMAGE", "calof/opeva_simulator:latest"))
    parser.add_argument("--poll-interval", type=float, default=float(os.environ.get("POLL_INTERVAL", "5")))
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=float(os.environ.get("WORKER_HEARTBEAT_INTERVAL", "30")),
    )
    parser.add_argument(
        "--status-poll-interval",
        type=float,
        default=float(os.environ.get("STATUS_POLL_INTERVAL", "10")),
    )
    parser.add_argument(
        "--exit-after-job",
        action="store_true",
        default=_env_flag("WORKER_EXIT_AFTER_JOB", False),
        help="Stop the worker after completing the next job",
    )
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
