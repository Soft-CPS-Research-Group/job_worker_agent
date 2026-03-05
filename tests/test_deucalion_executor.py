import time
from pathlib import Path

import yaml

from worker_agent.agent import WorkerAgent
from worker_agent.deucalion.slurm import SlurmState
import worker_agent.executors.deucalion_executor as deucalion_executor_module
from worker_agent.executors.deucalion_executor import DeucalionExecutor
from worker_agent.deucalion.ssh_client import SSHCommandError


class DummyResponse:
    def __init__(self, status_code, data=None):
        self.status_code = status_code
        self._data = data or {}

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self):
        self.calls = []
        self.status_responses = []

    def post(self, url, json=None, timeout=None):  # noqa: A003 - keep requests signature
        self.calls.append({"url": url, "json": json})
        return DummyResponse(200, {})

    def get(self, url, timeout=None):
        self.calls.append({"url": url, "json": None})
        if self.status_responses:
            value = self.status_responses.pop(0)
            return DummyResponse(200, {"status": value})
        return DummyResponse(200, {"status": "running"})


class FakeSSHClient:
    def __init__(self):
        self.commands = []
        self.copy_to_calls = []
        self.copy_from_calls = []

    def run(self, command, timeout=60, check=True):
        self.commands.append(command)
        if "&& echo yes || true" in command:
            return ""
        if command.startswith("cat "):
            return "line-from-slurm\n"
        return ""

    def copy_to(self, local_path, remote_path, timeout=60):
        self.copy_to_calls.append((str(local_path), remote_path))

    def copy_from(self, remote_path, local_path, timeout=60, recursive=False):
        self.copy_from_calls.append((remote_path, str(local_path), recursive))


def _write_config(shared_dir: Path, content: dict):
    config_path = shared_dir / "configs" / "demo.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(content), encoding="utf-8")
    return config_path


def test_deucalion_executor_happy_path(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient()

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
                    "gpus": 0,
                }
            }
        },
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "12345")
    states = iter(
        [
            SlurmState(state="PENDING"),
            SlurmState(state="RUNNING"),
            SlurmState(state="COMPLETED", exit_code=0),
        ]
    )
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: next(states))

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="deucalion",
        shared_dir=str(shared_dir),
        image="unused",
        session=session,
        executor="deucalion",
        status_poll_interval=0.0,
        deucalion_executor_factory=lambda runtime: DeucalionExecutor(
            runtime,
            env={"DEUCALION_SYNC_INTERVAL": "0"},
            ssh_client=fake_ssh,
            sleep_fn=lambda _x: None,
            now_fn=time.monotonic,
        ),
    )

    agent._run_job({"job_id": "job-1", "config_path": "configs/demo.yaml", "job_name": "Demo"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    statuses = [call["status"] for call in status_calls]
    assert "dispatched" in statuses
    assert "running" in statuses
    assert statuses[-1] == "finished"
    assert any(call.get("details", {}).get("slurm_job_id") == "12345" for call in status_calls)

    log_path = shared_dir / "jobs" / "job-1" / "logs" / "job-1.log"
    assert log_path.exists()
    assert "line-from-slurm" in log_path.read_text()


def test_deucalion_executor_stop_requested(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    session.status_responses = ["stop_requested"]
    fake_ssh = FakeSSHClient()
    cancel_called = {"value": False}

    _write_config(
        shared_dir,
        {"execution": {"deucalion": {"sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif"}}},
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "555")
    states = iter([SlurmState(state="RUNNING"), SlurmState(state="CANCELLED", exit_code=137)])
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: next(states))

    def _fake_cancel(*args, **kwargs):
        cancel_called["value"] = True

    monkeypatch.setattr(deucalion_executor_module, "scancel_job", _fake_cancel)

    agent = WorkerAgent(
        server_url="http://server",
        worker_id="deucalion",
        shared_dir=str(shared_dir),
        image="unused",
        session=session,
        executor="deucalion",
        status_poll_interval=0.01,
        deucalion_executor_factory=lambda runtime: DeucalionExecutor(
            runtime,
            env={"DEUCALION_SYNC_INTERVAL": "0"},
            ssh_client=fake_ssh,
            sleep_fn=lambda _x: None,
            now_fn=time.monotonic,
        ),
    )

    agent._run_job({"job_id": "job-stop", "config_path": "configs/demo.yaml", "job_name": "Stop"})

    assert cancel_called["value"] is True
    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "stopped"


def test_deucalion_executor_unreachable_timeout(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient()

    _write_config(
        shared_dir,
        {"execution": {"deucalion": {"sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif"}}},
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "999")
    monkeypatch.setattr(
        deucalion_executor_module,
        "query_state",
        lambda *args, **kwargs: (_ for _ in ()).throw(SSHCommandError("network down")),
    )

    now_values = iter([0.0, 1.0, 3.0, 5.0, 7.0])
    agent = WorkerAgent(
        server_url="http://server",
        worker_id="deucalion",
        shared_dir=str(shared_dir),
        image="unused",
        session=session,
        executor="deucalion",
        status_poll_interval=0.0,
        deucalion_executor_factory=lambda runtime: DeucalionExecutor(
            runtime,
            env={
                "DEUCALION_SYNC_INTERVAL": "0",
                "DEUCALION_UNREACHABLE_GRACE_SECONDS": "1",
            },
            ssh_client=fake_ssh,
            sleep_fn=lambda _x: None,
            now_fn=lambda: next(now_values),
        ),
    )

    agent._run_job({"job_id": "job-timeout", "config_path": "configs/demo.yaml", "job_name": "Timeout"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "failed"
    assert status_calls[-1]["error"] == "deucalion_unreachable_timeout"
