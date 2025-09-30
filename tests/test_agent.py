import json
import time
from pathlib import Path

import pytest

from worker_agent.agent import WorkerAgent


class DummyResponse:
    def __init__(self, status_code, data=None):
        self.status_code = status_code
        self._data = data

    def json(self):
        if self._data is None:
            raise ValueError("No JSON content")
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self):
        self.calls = []
        self.next_job_responses = []
        self.status_responses = []

    def post(self, url, json=None, timeout=None):  # noqa: A003 json parameter name is intentional
        self.calls.append({"url": url, "json": json})
        if url.endswith("/heartbeat"):
            return DummyResponse(200, {})
        if url.endswith("/job-status"):
            return DummyResponse(200, {})
        if url.endswith("/next-job"):
            if self.next_job_responses:
                return self.next_job_responses.pop(0)
            return DummyResponse(204, {})
        return DummyResponse(200, {})

    def get(self, url, timeout=None):
        self.calls.append({"url": url, "json": None})
        if self.status_responses:
            status = self.status_responses.pop(0)
            return DummyResponse(200, {"status": status})
        return DummyResponse(200, {"status": "running"})


class DummyContainer:
    def __init__(self, exit_code=0, logs=None):
        self.id = "cid-123"
        self.name = "container-name"
        self._exit_code = exit_code
        self._logs = logs or [b"line1\n"]
        self.removed = False
        self.stop_called = False

    def logs(self, stream=True, follow=True):
        for chunk in self._logs:
            yield chunk

    def wait(self):
        return {"StatusCode": self._exit_code}

    def remove(self, force=True):
        self.removed = True

    def stop(self):
        self.stop_called = True


class DummyDockerClient:
    def __init__(self, container):
        self.container = container
        self.containers = self
        self.last_kwargs = None

    def run(self, **kwargs):
        self.last_kwargs = kwargs
        return self.container

    def close(self):
        pass


def test_run_job_success(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    container = DummyContainer(exit_code=0, logs=[b"hello\n"])
    docker_client = DummyDockerClient(container)

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="worker-a",
        shared_dir=str(shared_dir),
        image="my-image",
        session=session,
        docker_client_factory=lambda: docker_client,
        poll_interval=0.1,
        heartbeat_interval=0.1,
        status_poll_interval=0.0,
    )

    job = {"job_id": "job1", "config_path": "configs/demo.yaml", "job_name": "Demo"}
    agent._run_job(job)

    status_calls = [call for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[0]["json"]["status"] == "running"
    assert status_calls[-1]["json"]["status"] == "finished"

    log_path = shared_dir / "jobs" / "job1" / "logs" / "job1.log"
    assert log_path.exists()
    assert "hello" in log_path.read_text()

    assert docker_client.last_kwargs["image"] == "my-image"
    assert "--config /data/configs/demo.yaml" in docker_client.last_kwargs["command"]


def test_run_job_failure(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    container = DummyContainer(exit_code=5, logs=[b"oops\n"])
    docker_client = DummyDockerClient(container)

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="worker-a",
        shared_dir=str(shared_dir),
        image="my-image",
        session=session,
        docker_client_factory=lambda: docker_client,
        status_poll_interval=0.0,
    )

    agent._run_job({"job_id": "job2", "config_path": "cfg.yaml", "job_name": "Demo"})

    status_calls = [call for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["json"]["status"] == "failed"


def test_poll_once_dispatches_job(monkeypatch):
    session = DummySession()
    job_payload = {"job_id": "job9", "config_path": "cfg.yaml", "job_name": "Demo"}
    session.next_job_responses.append(DummyResponse(200, job_payload))

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="worker-a",
        shared_dir="/tmp",
        image="img",
        session=session,
        docker_client_factory=lambda: DummyDockerClient(DummyContainer()),
        heartbeat_interval=0,
        status_poll_interval=0.0,
    )

    handled = agent.poll_once()
    assert handled is True


def test_exit_after_job_stops_worker(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    job_payload = {"job_id": "job-exit", "config_path": "cfg.yaml", "job_name": "Demo"}
    session.next_job_responses.append(DummyResponse(200, job_payload))
    container = DummyContainer(exit_code=0)
    docker_client = DummyDockerClient(container)

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="worker-a",
        shared_dir=str(shared_dir),
        image="img",
        session=session,
        docker_client_factory=lambda: docker_client,
        exit_after_job=True,
        status_poll_interval=0.0,
        heartbeat_interval=0,
    )

    handled = agent.poll_once()
    assert handled is True
    assert agent._stop_event.is_set() is True


def test_request_exit_after_current_job_when_idle(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    container = DummyContainer(exit_code=0)
    docker_client = DummyDockerClient(container)

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="worker-a",
        shared_dir=str(shared_dir),
        image="img",
        session=session,
        docker_client_factory=lambda: docker_client,
        status_poll_interval=0.0,
        heartbeat_interval=0,
    )

    agent.request_exit_after_current_job()
    assert agent._stop_event.is_set() is True


def test_poll_once_no_job(monkeypatch):
    session = DummySession()
    agent = WorkerAgent(
        server_url="http://server",
        worker_id="worker-a",
        shared_dir="/tmp",
        image="img",
        session=session,
        docker_client_factory=lambda: DummyDockerClient(DummyContainer()),
        heartbeat_interval=0,
        status_poll_interval=0.0,
    )

    handled = agent.poll_once()
    assert handled is False
    # heartbeat was sent
    assert any(call for call in session.calls if call["url"].endswith("/heartbeat"))


def test_run_job_canceled(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    session.status_responses = ["running", "canceled"]

    class CancelContainer(DummyContainer):
        def __init__(self):
            super().__init__(exit_code=137, logs=[b"start\n"])

        def wait(self):
            for _ in range(20):
                if self.stop_called:
                    return {"StatusCode": 137}
                time.sleep(0.01)
            return {"StatusCode": 0}

    container = CancelContainer()
    docker_client = DummyDockerClient(container)

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="worker-a",
        shared_dir=str(shared_dir),
        image="my-image",
        session=session,
        docker_client_factory=lambda: docker_client,
        status_poll_interval=0.05,
        heartbeat_interval=0.0,
    )

    agent._run_job({"job_id": "job3", "config_path": "cfg.yaml", "job_name": "Demo"})

    assert container.stop_called is True
    status_calls = [call for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["json"]["status"] == "canceled"
