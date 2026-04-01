import json
import time
import threading
from pathlib import Path

import pytest
import requests

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
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class DummySession:
    def __init__(self):
        self.calls = []
        self.next_job_responses = []
        self.status_responses = []
        self.heartbeat_responses = []
        self.job_status_post_responses = []

    def post(self, url, json=None, timeout=None):  # noqa: A003 json parameter name is intentional
        self.calls.append({"url": url, "json": json})
        if url.endswith("/heartbeat"):
            if self.heartbeat_responses:
                return self.heartbeat_responses.pop(0)
            return DummyResponse(200, {})
        if url.endswith("/job-status"):
            if self.job_status_post_responses:
                return self.job_status_post_responses.pop(0)
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


class PullTrackingImages:
    def __init__(self):
        self.pulled = []

    def pull(self, image):
        self.pulled.append(image)


class PullTrackingDockerClient(DummyDockerClient):
    def __init__(self, container):
        super().__init__(container)
        self.images = PullTrackingImages()


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
    assert docker_client.last_kwargs["labels"]["opeva.job_id"] == "job1"
    assert docker_client.last_kwargs["labels"]["opeva.worker_id"] == "worker-a"


def test_run_job_pulls_image_before_start(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKER_DOCKER_PULL_POLICY", "always")
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    container = DummyContainer(exit_code=0, logs=[b"hello\n"])
    docker_client = PullTrackingDockerClient(container)

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

    job = {"job_id": "job-pull", "config_path": "configs/demo.yaml", "job_name": "Demo", "image": "calof/algorithms:v1"}
    agent._run_job(job)

    assert docker_client.images.pulled == ["calof/algorithms:v1"]


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


def test_run_job_stop_requested(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    session.status_responses = ["running", "stop_requested"]

    class StopContainer(DummyContainer):
        def __init__(self):
            super().__init__(exit_code=137, logs=[b"start\n"])

        def wait(self):
            for _ in range(20):
                if self.stop_called:
                    return {"StatusCode": 137}
                time.sleep(0.01)
            return {"StatusCode": 0}

    container = StopContainer()
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

    agent._run_job({"job_id": "job-stop", "config_path": "cfg.yaml", "job_name": "Demo"})

    assert container.stop_called is True
    status_calls = [call for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["json"]["status"] == "stopped"


def test_run_job_stops_when_backend_requeued(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    session.status_responses = ["running", "queued"]

    class RequeueContainer(DummyContainer):
        def __init__(self):
            super().__init__(exit_code=137, logs=[b"start\n"])

        def wait(self):
            for _ in range(20):
                if self.stop_called:
                    return {"StatusCode": 137}
                time.sleep(0.01)
            return {"StatusCode": 0}

    container = RequeueContainer()
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

    agent._run_job({"job_id": "job-requeue", "config_path": "cfg.yaml", "job_name": "Demo"})

    assert container.stop_called is True
    status_calls = [call["json"]["status"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls
    assert all(status == "running" for status in status_calls)


def test_run_job_stops_when_backend_failed(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    session.status_responses = ["running", "failed"]

    class FailedContainer(DummyContainer):
        def __init__(self):
            super().__init__(exit_code=137, logs=[b"start\n"])

        def wait(self):
            for _ in range(20):
                if self.stop_called:
                    return {"StatusCode": 137}
                time.sleep(0.01)
            return {"StatusCode": 0}

    container = FailedContainer()
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

    agent._run_job({"job_id": "job-fail-override", "config_path": "cfg.yaml", "job_name": "Demo"})

    assert container.stop_called is True
    status_calls = [call["json"]["status"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls
    assert all(status == "running" for status in status_calls)


def test_heartbeat_retries_and_updates_timestamp_only_on_success(monkeypatch):
    session = DummySession()
    session.heartbeat_responses = [
        DummyResponse(500, {}),
        DummyResponse(503, {}),
        DummyResponse(200, {}),
    ]
    monkeypatch.setattr("worker_agent.agent.time.sleep", lambda _seconds: None)

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
    assert agent._last_heartbeat == 0.0

    agent._send_heartbeat(force=True)

    heartbeat_calls = [call for call in session.calls if call["url"].endswith("/heartbeat")]
    assert len(heartbeat_calls) == 3
    assert agent._last_heartbeat > 0


def test_terminal_status_is_buffered_and_flushed_after_retryable_failures(monkeypatch):
    session = DummySession()
    session.job_status_post_responses = [
        DummyResponse(500, {}),
        DummyResponse(503, {}),
        DummyResponse(502, {}),
        DummyResponse(200, {}),
    ]
    monkeypatch.setattr("worker_agent.agent.time.sleep", lambda _seconds: None)

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

    agent._post_status("job-buffer", "finished", exit_code=0)
    assert len(agent._pending_terminal_statuses) == 1

    agent._flush_pending_terminal_statuses(force=True)
    assert len(agent._pending_terminal_statuses) == 0

    status_calls = [call for call in session.calls if call["url"].endswith("/job-status")]
    assert len(status_calls) == 4


def test_terminal_status_non_retryable_4xx_is_not_buffered():
    session = DummySession()
    session.job_status_post_responses = [DummyResponse(400, {})]

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

    agent._post_status("job-4xx", "finished", exit_code=0)
    assert len(agent._pending_terminal_statuses) == 0

    status_calls = [call for call in session.calls if call["url"].endswith("/job-status")]
    assert len(status_calls) == 1


def test_heartbeat_payload_contains_runtime_metadata():
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
    agent._mark_active_job("job-meta")
    agent._post_status("job-meta", "running")
    agent._post_status("job-meta", "finished", exit_code=0)
    agent._mark_active_job(None)
    agent._send_heartbeat(force=True)

    heartbeat_calls = [call for call in session.calls if call["url"].endswith("/heartbeat")]
    assert heartbeat_calls
    payload = heartbeat_calls[-1]["json"]
    info = payload["info"]
    assert payload["worker_id"] == "worker-a"
    assert info["executor"] == "docker"
    assert isinstance(info["worker_version"], str)
    assert info["active_job_id"] is None
    assert info["active_job_count"] == 0
    assert info["last_job_id"] == "job-meta"
    assert info["last_terminal_status"] == "finished"


def test_deucalion_defaults_to_three_active_slots():
    session = DummySession()

    class NoopExecutor:
        def run_job(self, job):  # pragma: no cover - should not run in this test
            return None

        def heartbeat_info(self):
            return {}

        def close(self):
            return None

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="deucalion",
        shared_dir="/tmp",
        image="img",
        session=session,
        executor="deucalion",
        deucalion_executor_factory=lambda runtime: NoopExecutor(),
        heartbeat_interval=0,
        status_poll_interval=0.0,
    )

    assert agent.max_active_jobs == 3


def test_deucalion_poll_once_fills_only_available_slots():
    session = DummySession()
    release_jobs = threading.Event()
    started_jobs: list[str] = []

    class BlockingExecutor:
        def __init__(self, runtime):
            self.runtime = runtime

        def run_job(self, job):
            job_id = job["job_id"]
            started_jobs.append(job_id)
            self.runtime._register_active_job(job_id)
            self.runtime._update_active_job(job_id, status="dispatched", phase="execution:dispatched")
            release_jobs.wait(timeout=2)
            self.runtime._unregister_active_job(job_id)

        def heartbeat_info(self):
            return {}

        def close(self):
            return None

    for idx in range(1, 5):
        session.next_job_responses.append(
            DummyResponse(200, {"job_id": f"job-{idx}", "config_path": "cfg.yaml", "job_name": f"Demo {idx}"})
        )

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="deucalion",
        shared_dir="/tmp",
        image="img",
        session=session,
        executor="deucalion",
        deucalion_executor_factory=lambda runtime: BlockingExecutor(runtime),
        env={"DEUCALION_MAX_ACTIVE_JOBS": "3"},
        heartbeat_interval=0,
        status_poll_interval=0.0,
    )

    handled = agent.poll_once()
    assert handled is True
    assert len(started_jobs) == 3
    assert agent._running_thread_count() == 3

    next_job_calls = [call for call in session.calls if call["url"].endswith("/next-job")]
    assert len(next_job_calls) == 3

    release_jobs.set()
    agent._join_job_threads(timeout=2)
    assert agent._running_thread_count() == 0


def test_heartbeat_payload_includes_multiple_active_jobs_details():
    session = DummySession()

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="deucalion",
        shared_dir="/tmp",
        image="img",
        session=session,
        executor="deucalion",
        deucalion_executor_factory=lambda runtime: type(
            "NoopExecutor",
            (),
            {"run_job": lambda self, job: None, "heartbeat_info": lambda self: {}, "close": lambda self: None},
        )(),
        heartbeat_interval=0,
        status_poll_interval=0.0,
    )

    agent._register_active_job("job-a")
    agent._update_active_job(
        "job-a",
        status="dispatched",
        phase="execution:poll",
        slurm_job_id="101",
        slurm_state="PENDING",
        queue_pos=9,
        ahead=8,
    )
    agent._register_active_job("job-b")
    agent._update_active_job(
        "job-b",
        status="running",
        phase="execution:poll",
        slurm_job_id="102",
        slurm_state="RUNNING",
    )

    agent._send_heartbeat(force=True)

    heartbeat_calls = [call for call in session.calls if call["url"].endswith("/heartbeat")]
    assert heartbeat_calls
    info = heartbeat_calls[-1]["json"]["info"]

    assert info["active_job_count"] == 2
    assert set(info["active_job_ids"]) == {"job-a", "job-b"}
    rows = {row["job_id"]: row for row in info["active_jobs"]}
    assert rows["job-a"]["slurm_state"] == "PENDING"
    assert rows["job-a"]["queue_pos"] == 9
    assert rows["job-a"]["ahead"] == 8
    assert rows["job-b"]["slurm_state"] == "RUNNING"
