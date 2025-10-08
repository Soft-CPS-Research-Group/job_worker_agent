import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pytest

from worker_agent.agent import WorkerAgent


@dataclass
class ScriptedJob:
    job_id: str
    config_path: str
    job_name: str
    exit_code: int
    logs: List[bytes]
    status_sequence: List[str] = field(default_factory=list)

    def payload(self) -> Dict[str, str]:
        return {
            "job_id": self.job_id,
            "config_path": self.config_path,
            "job_name": self.job_name,
        }


class FakeResponse:
    def __init__(self, status_code: int, data: Optional[Dict[str, str]] = None):
        self.status_code = status_code
        self._data = data or {}

    def json(self) -> Dict[str, str]:
        return self._data

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            raise RuntimeError(f"HTTP {self.status_code}")


class ScriptedBackendSession:
    def __init__(self, jobs: List[ScriptedJob]):
        self._jobs = list(jobs)
        self._lock = threading.Lock()
        self._job_queue: List[ScriptedJob] = list(jobs)
        self.status_sequences: Dict[str, List[str]] = {
            job.job_id: list(job.status_sequence) for job in jobs
        }
        self.last_status: Dict[str, str] = {job.job_id: "running" for job in jobs}
        self.heartbeats: List[Dict[str, str]] = []
        self.job_status_posts: List[Dict[str, str]] = []
        self.next_job_calls = 0
        self.status_calls: Dict[str, int] = {job.job_id: 0 for job in jobs}
        self._final_jobs_reported: set[str] = set()
        self.all_jobs_done = threading.Event()

    def post(self, url: str, json: Optional[Dict[str, str]] = None, timeout: Optional[float] = None):
        if url.endswith("/heartbeat"):
            if json is not None:
                self.heartbeats.append(json)
            return FakeResponse(200, {})
        if url.endswith("/next-job"):
            with self._lock:
                self.next_job_calls += 1
                if self._job_queue:
                    job = self._job_queue.pop(0)
                    return FakeResponse(200, job.payload())
            return FakeResponse(204, {})
        if url.endswith("/job-status"):
            if json is not None:
                self.job_status_posts.append(json)
                status = json.get("status")
                job_id = json.get("job_id")
                if status in {"finished", "failed", "canceled", "stopped"} and job_id:
                    self._final_jobs_reported.add(job_id)
                    if len(self._final_jobs_reported) == len(self._jobs):
                        self.all_jobs_done.set()
            return FakeResponse(200, {})
        return FakeResponse(200, {})

    def get(self, url: str, timeout: Optional[float] = None):
        job_id = url.rsplit("/", 1)[-1]
        if job_id not in self.status_sequences:
            return FakeResponse(404, {})
        queue = self.status_sequences[job_id]
        if queue:
            status = queue.pop(0)
            self.last_status[job_id] = status
        else:
            status = self.last_status[job_id]
        self.status_calls[job_id] += 1
        return FakeResponse(200, {"status": status})


class ScriptedContainer:
    def __init__(self, job: ScriptedJob):
        self._job = job
        self.id = f"cid-{job.job_id}"
        self.name = job.job_name
        self.stop_called = threading.Event()
        self.removed = False

    def logs(self, stream: bool = True, follow: bool = True):
        for chunk in self._job.logs:
            yield chunk

    def wait(self):
        if self.stop_called.wait(timeout=0.5):
            return {"StatusCode": self._job.exit_code}
        time.sleep(0.05)
        return {"StatusCode": self._job.exit_code}

    def stop(self):
        self.stop_called.set()

    def remove(self, force: bool = True):
        self.removed = True


class ScriptedDockerClient:
    def __init__(self, jobs: List[ScriptedJob], fail_on_gpu: bool = False):
        self._containers: Dict[str, ScriptedContainer] = {
            job.job_id: ScriptedContainer(job) for job in jobs
        }
        self.run_calls: List[Dict[str, object]] = []
        self.containers = self
        self.fail_on_gpu = fail_on_gpu

    def run(self, **kwargs):
        command = kwargs.get("command", "")
        job_id = self._extract_job_id(command)
        container = self._containers[job_id]
        container.name = kwargs.get("name", container.name)
        device_requests = kwargs.get("device_requests")
        self.run_calls.append(
            {
                "job_id": job_id,
                "command": command,
                "volumes": kwargs.get("volumes"),
                "image": kwargs.get("image"),
                "labels": kwargs.get("labels"),
                "device_requests": device_requests,
            }
        )
        if self.fail_on_gpu and device_requests:
            raise RuntimeError("device requests not supported")
        return container

    def _extract_job_id(self, command: str) -> str:
        parts = command.split()
        for flag in ("--job-id", "--job_id"):
            if flag in parts:
                idx = parts.index(flag)
                if idx + 1 < len(parts):
                    return parts[idx + 1]
        raise AssertionError(f"Job id not found in command: {command}")

    def close(self):
        return None


def test_worker_run_forever_integration(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()

    jobs = [
        ScriptedJob(
            job_id="job-success",
            config_path="cfg/success.yaml",
            job_name="Success Job",
            exit_code=0,
            logs=[b"success line\n"],
            status_sequence=[],
        ),
        ScriptedJob(
            job_id="job-cancel",
            config_path="cfg/cancel.yaml",
            job_name="Cancel Job",
            exit_code=137,
            logs=[b"starting\n"],
            status_sequence=["running", "canceled"],
        ),
    ]

    backend = ScriptedBackendSession(jobs)
    docker_client = ScriptedDockerClient(jobs)

    agent = WorkerAgent(
        server_url="http://backend",
        worker_id="worker-int",
        shared_dir=str(shared_dir),
        image="test-image",
        poll_interval=0.01,
        heartbeat_interval=0.01,
        status_poll_interval=0.02,
        session=backend,
        docker_client_factory=lambda: docker_client,
    )

    thread = threading.Thread(target=agent.run_forever, daemon=True)
    thread.start()

    assert backend.all_jobs_done.wait(timeout=2), "expected all jobs to finish"
    agent.stop()
    thread.join(timeout=2)
    assert not thread.is_alive(), "worker thread should exit after stop"

    # Heartbeat: initial poll and force heartbeat after jobs complete.
    assert backend.heartbeats, "heartbeat should be posted"

    # Status transitions captured for both jobs.
    job_status_by_id = {}
    for call in backend.job_status_posts:
        job_status_by_id.setdefault(call["job_id"], []).append(call)

    assert [entry["status"] for entry in job_status_by_id["job-success"]] == ["running", "finished"]
    assert [entry["status"] for entry in job_status_by_id["job-cancel"]][-1] == "canceled"

    cancel_final = job_status_by_id["job-cancel"][-1]
    assert cancel_final["exit_code"] == 137

    # Containers should have been started with correct command and volume mapping.
    run_commands = {call["job_id"]: call for call in docker_client.run_calls}
    assert "--config /data/cfg/success.yaml" in run_commands["job-success"]["command"]
    assert "--job_id job-success" in run_commands["job-success"]["command"]
    assert run_commands["job-success"]["volumes"] == {str(shared_dir): {"bind": "/data", "mode": "rw"}}
    assert run_commands["job-success"]["labels"]["opeva.job_id"] == "job-success"
    assert run_commands["job-success"]["labels"]["opeva.worker_id"] == "worker-int"

    # Log files created for each job.
    success_log = shared_dir / "jobs" / "job-success" / "logs" / "job-success.log"
    cancel_log = shared_dir / "jobs" / "job-cancel" / "logs" / "job-cancel.log"
    assert success_log.exists()
    assert cancel_log.exists()
    assert "success line" in success_log.read_text()
    assert "starting" in cancel_log.read_text()

    # Ensure cancellation triggered container.stop.
    cancel_container = docker_client._containers["job-cancel"]
    assert cancel_container.stop_called.is_set(), "cancel container should receive stop()"
    assert cancel_container.removed is True


def test_worker_gpu_auto_fallback(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()

    job = ScriptedJob(
        job_id="job-gpu",
        config_path="cfg/gpu.yaml",
        job_name="Job GPU",
        exit_code=0,
        logs=[b"gpu job\n"],
        status_sequence=[],
    )

    backend = ScriptedBackendSession([job])
    docker_client = ScriptedDockerClient([job], fail_on_gpu=True)
    agent = WorkerAgent(
        server_url="http://backend",
        worker_id="worker-gpu",
        shared_dir=str(shared_dir),
        image="test-image",
        poll_interval=0.01,
        heartbeat_interval=0.01,
        status_poll_interval=0.02,
        session=backend,
        docker_client_factory=lambda: docker_client,
    )
    agent._gpu_request_enabled = True  # type: ignore[attr-defined]
    agent._build_device_requests = lambda: ["gpu-request"]  # type: ignore[assignment]

    thread = threading.Thread(target=agent.run_forever, daemon=True)
    thread.start()
    assert backend.all_jobs_done.wait(timeout=2)
    agent.stop()
    thread.join(timeout=2)

    assert len(docker_client.run_calls) >= 2
    assert docker_client.run_calls[0]["device_requests"], "first attempt should request GPU"
    assert docker_client.run_calls[-1]["device_requests"] is None, "fallback should omit GPU request"
