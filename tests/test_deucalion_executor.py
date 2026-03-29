import re
import time
from pathlib import Path

import yaml

from worker_agent.agent import WorkerAgent
from worker_agent.deucalion.slurm import SlurmState
from worker_agent.deucalion.ssh_client import SSHCommandError
from worker_agent.executors.deucalion_executor import DeucalionExecutor
import worker_agent.executors.deucalion_executor as deucalion_executor_module


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

    def post(self, url, json=None, timeout=None):  # noqa: A003
        self.calls.append({"url": url, "json": json})
        return DummyResponse(200, {})

    def get(self, url, timeout=None):
        self.calls.append({"url": url, "json": None})
        if self.status_responses:
            value = self.status_responses.pop(0)
            return DummyResponse(200, {"status": value})
        return DummyResponse(200, {"status": "running"})


class FakeSSHClient:
    def __init__(self, existing_paths=None, copy_from_failures=None):
        self.commands = []
        self.copy_to_calls = []
        self.copy_from_calls = []
        self.remote_files = {}
        self.existing_paths = set(existing_paths or [])
        self.copy_from_failures = dict(copy_from_failures or {})

    def _exists(self, path: str) -> bool:
        if path in self.existing_paths or path in self.remote_files:
            return True
        prefix = path.rstrip("/") + "/"
        return any(p.startswith(prefix) for p in self.existing_paths | set(self.remote_files.keys()))

    def run(self, command, timeout=60, check=True):
        self.commands.append(command)

        m = re.match(r"test -[edf] (\S+) && echo yes \|\| true$", command)
        if m:
            path = m.group(1).strip("'\"")
            return "yes" if self._exists(path) else ""

        m = re.match(r"test -[edf] (\S+)$", command)
        if m:
            path = m.group(1).strip("'\"")
            if self._exists(path):
                return ""
            if check:
                raise SSHCommandError("missing path")
            return ""

        if command.startswith("mkdir -p "):
            for part in command[len("mkdir -p ") :].split():
                self.existing_paths.add(part.strip("'\""))
            return ""

        if command.startswith("ln -sfn "):
            parts = command.split()
            target = parts[2].strip("'\"")
            link = parts[3].strip("'\"")
            self.existing_paths.add(link)
            self.remote_files[link] = f"symlink->{target}"
            return ""

        m = re.match(r"if \[ -f (\S+) \]; then wc -c < (\S+); else echo 0; fi$", command)
        if m:
            path = m.group(1).strip("'\"")
            return str(len(self.remote_files.get(path, "")))

        m = re.match(r"if \[ -f (\S+) \]; then tail -c \+(\d+) (\S+); fi$", command)
        if m:
            path = m.group(1).strip("'\"")
            start = int(m.group(2))
            content = self.remote_files.get(path, "")
            idx = max(0, start - 1)
            return content[idx:]

        m = re.match(r"if \[ -f (\S+) \]; then cat (\S+); fi$", command)
        if m:
            path = m.group(1).strip("'\"")
            return self.remote_files.get(path, "")

        if " pull --force " in command and " docker://" in command:
            m = re.search(r"pull --force (\S+) docker://", command)
            if m:
                sif_path = m.group(1).strip("'\"")
                self.existing_paths.add(sif_path)
                self.remote_files[sif_path] = "sif-binary"
            return ""

        return ""

    def copy_to(self, local_path, remote_path, timeout=60, recursive=False):
        self.copy_to_calls.append((str(local_path), remote_path, recursive))
        self.existing_paths.add(remote_path)
        lp = Path(local_path)
        if lp.exists() and lp.is_file():
            self.remote_files[remote_path] = lp.read_text(encoding="utf-8")

    def copy_from(self, remote_path, local_path, timeout=60, recursive=False):
        remaining_failures = self.copy_from_failures.get(remote_path, 0)
        if remaining_failures > 0:
            self.copy_from_failures[remote_path] = remaining_failures - 1
            raise SSHCommandError(f"copy failed for {remote_path}")
        self.copy_from_calls.append((remote_path, str(local_path), recursive))
        if not recursive and remote_path in self.remote_files:
            lp = Path(local_path)
            lp.parent.mkdir(parents=True, exist_ok=True)
            lp.write_text(self.remote_files[remote_path], encoding="utf-8")


class BudgetSSHClient(FakeSSHClient):
    def __init__(self):
        super().__init__()
        self.billing_calls = 0

    def run(self, command, timeout=60, check=True):
        if command == "billing":
            self.billing_calls += 1
            return (
                "┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┓\n"
                "┃ Account           ┃ Used (h) ┃ Limit (h) ┃ Used (%) ┃\n"
                "┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━┩\n"
                "│ f202508843cpcaa0g │        0 │       700 │     0.00 │\n"
                "│ f202508843cpcaa0x │    10752 │     48000 │    22.40 │\n"
                "└───────────────────┴──────────┴───────────┴──────────┘\n"
            )
        return super().run(command, timeout=timeout, check=check)


def _write_config(shared_dir: Path, content: dict):
    config_path = shared_dir / "configs" / "demo.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(content), encoding="utf-8")
    return config_path


def _build_agent(shared_dir: Path, session: DummySession, fake_ssh: FakeSSHClient, env: dict | None = None, now_fn=None):
    return WorkerAgent(
        server_url="http://server",
        worker_id="deucalion",
        shared_dir=str(shared_dir),
        image="unused",
        session=session,
        executor="deucalion",
        status_poll_interval=0.01,
        deucalion_executor_factory=lambda runtime: DeucalionExecutor(
            runtime,
            env=env or {"DEUCALION_SYNC_INTERVAL": "0"},
            ssh_client=fake_ssh,
            sleep_fn=lambda _x: None,
            now_fn=now_fn or time.monotonic,
        ),
    )


def test_deucalion_executor_happy_path_default_run_and_incremental_logs(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )

    job_id = "job-1"
    remote_job_dir = f"/projects/F202508843CPCAA0/tiagocalof/runs/{job_id}"
    remote_data_job_dir = f"{remote_job_dir}/data/jobs/{job_id}"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.out"] = "line-from-slurm\n"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.err"] = "err-line\n"
    fake_ssh.existing_paths.update(
        {
            f"{remote_data_job_dir}/results",
            f"{remote_data_job_dir}/progress",
        }
    )

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
                }
            }
        },
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "12345")
    states = iter(
        [
            SlurmState(state="PENDING"),
            SlurmState(state="RUNNING"),
            SlurmState(state="RUNNING"),
            SlurmState(state="COMPLETED", exit_code=0),
        ]
    )
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: next(states))

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": job_id, "config_path": "configs/demo.yaml", "job_name": "Demo"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "finished"
    assert status_calls[0]["details"]["command_mode"] == "run"

    # sbatch script uses singularity run by default
    sbatch_remote = f"{remote_job_dir}/run.sbatch"
    assert "singularity run" in fake_ssh.remote_files[sbatch_remote]

    # log sync is incremental: content appears once even with multiple sync loops
    log_path = shared_dir / "jobs" / job_id / "logs" / f"{job_id}.log"
    log_text = log_path.read_text(encoding="utf-8")
    assert log_text.count("line-from-slurm") == 1
    assert log_text.count("err-line") == 1
    copied_from = [remote for remote, _local, _recursive in fake_ssh.copy_from_calls]
    assert f"{remote_data_job_dir}/results" in copied_from
    assert f"{remote_data_job_dir}/progress" in copied_from


def test_deucalion_executor_refreshes_sif_when_version_changes(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    sif_path = "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif"
    marker_path = f"{sif_path}.version"
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            sif_path,
            marker_path,
        }
    )
    fake_ssh.remote_files[marker_path] = "v0.2.4\n"

    job_id = "job-version-refresh"
    remote_job_dir = f"/projects/F202508843CPCAA0/tiagocalof/runs/{job_id}"
    remote_data_job_dir = f"{remote_job_dir}/data/jobs/{job_id}"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.out"] = "line-from-slurm\n"
    fake_ssh.existing_paths.update({f"{remote_data_job_dir}/results", f"{remote_data_job_dir}/progress"})

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": sif_path,
                    "sif_image": "calof/opeva_simulator",
                    "sif_version": "v0.2.5",
                }
            }
        },
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "12345")
    states = iter([SlurmState(state="PENDING"), SlurmState(state="COMPLETED", exit_code=0)])
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: next(states))

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": job_id, "config_path": "configs/demo.yaml", "job_name": "Demo"})

    # Worker should rebuild with the requested version tag.
    assert any("docker://calof/opeva_simulator:v0.2.5" in cmd for cmd in fake_ssh.commands)
    assert fake_ssh.remote_files[marker_path].strip() == "v0.2.5"


def test_deucalion_heartbeat_includes_budget_snapshot_and_uses_cache(tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = BudgetSSHClient()

    agent = _build_agent(
        shared_dir,
        session,
        fake_ssh,
        env={"DEUCALION_BUDGET_REFRESH_INTERVAL_SECONDS": "3600"},
    )
    agent._send_heartbeat(force=True)
    agent._send_heartbeat(force=True)

    heartbeat_calls = [call for call in session.calls if call["url"].endswith("/heartbeat")]
    assert heartbeat_calls
    info = heartbeat_calls[-1]["json"]["info"]
    assert info["executor"] == "deucalion"
    assert "budget" in info
    assert info["budget"]["accounts"][0]["account"] == "f202508843cpcaa0g"
    assert info["budget"]["accounts"][1]["used_percent"] == 22.4
    assert "budget_refreshed_at" in info
    assert fake_ssh.billing_calls == 1


def test_deucalion_executor_syncs_progress_snapshot_during_execution(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )

    job_id = "job-progress"
    remote_job_dir = f"/projects/F202508843CPCAA0/tiagocalof/runs/{job_id}"
    remote_progress_file = f"{remote_job_dir}/data/jobs/{job_id}/progress/progress.json"
    fake_ssh.remote_files[remote_progress_file] = '{"step": 12}'
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.out"] = "out\n"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.err"] = "err\n"

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
                }
            }
        },
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "12346")
    states = iter(
        [
            SlurmState(state="PENDING"),
            SlurmState(state="RUNNING"),
            SlurmState(state="COMPLETED", exit_code=0),
        ]
    )
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: next(states))

    agent = _build_agent(shared_dir, session, fake_ssh, env={"DEUCALION_SYNC_INTERVAL": "0"})
    agent._run_job({"job_id": job_id, "config_path": "configs/demo.yaml", "job_name": "Progress"})

    local_progress = shared_dir / "jobs" / job_id / "progress" / "progress.json"
    assert local_progress.exists()
    assert local_progress.read_text(encoding="utf-8") == '{"step": 12}'


def test_deucalion_executor_sync_fallback_to_legacy_artifact_paths(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )

    job_id = "job-legacy"
    remote_job_dir = f"/projects/F202508843CPCAA0/tiagocalof/runs/{job_id}"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.out"] = "legacy-out\n"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.err"] = "legacy-err\n"
    fake_ssh.existing_paths.update(
        {
            f"{remote_job_dir}/results",
            f"{remote_job_dir}/progress",
        }
    )

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
                }
            }
        },
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "54321")
    states = iter(
        [
            SlurmState(state="PENDING"),
            SlurmState(state="RUNNING"),
            SlurmState(state="COMPLETED", exit_code=0),
        ]
    )
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: next(states))

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": job_id, "config_path": "configs/demo.yaml", "job_name": "Legacy"})

    copied_from = [remote for remote, _local, _recursive in fake_ssh.copy_from_calls]
    assert f"{remote_job_dir}/results" in copied_from
    assert f"{remote_job_dir}/progress" in copied_from


def test_deucalion_executor_exec_mode_requires_executable(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
                    "command_mode": "exec",
                }
            }
        },
    )

    called = {"sbatch": False}

    def _fake_submit(*args, **kwargs):
        called["sbatch"] = True
        return "123"

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", _fake_submit)
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: SlurmState(state="COMPLETED", exit_code=0))

    agent = _build_agent(shared_dir, session, fake_ssh)
    # default command is only "--config ... --job_id ...", invalid for exec mode
    agent._run_job({"job_id": "job-exec", "config_path": "configs/demo.yaml", "job_name": "Exec"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "failed"
    assert "requires an explicit executable" in status_calls[-1]["error"]
    assert called["sbatch"] is False


def test_deucalion_executor_dataset_sync_copy_missing_skip_existing(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()

    (shared_dir / "datasets" / "site_a").mkdir(parents=True, exist_ok=True)
    (shared_dir / "datasets" / "site_a" / "a.csv").write_text("a", encoding="utf-8")
    (shared_dir / "datasets" / "site_b").mkdir(parents=True, exist_ok=True)
    (shared_dir / "datasets" / "site_b" / "b.csv").write_text("b", encoding="utf-8")

    existing = {
        "/projects/F202508843CPCAA0/tiagocalof",
        "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        "/projects/F202508843CPCAA0/tiagocalof/datasets/site_a/a.csv",
    }
    fake_ssh = FakeSSHClient(existing_paths=existing)

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
                    "datasets": [
                        "datasets/site_a/a.csv",
                        "datasets/site_b/b.csv",
                    ],
                }
            }
        },
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "777")
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: SlurmState(state="COMPLETED", exit_code=0))

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": "job-data", "config_path": "configs/demo.yaml", "job_name": "Data"})

    copied_remote_paths = [remote for _local, remote, _recursive in fake_ssh.copy_to_calls]
    assert "/projects/F202508843CPCAA0/tiagocalof/datasets/site_b/b.csv" in copied_remote_paths
    assert "/projects/F202508843CPCAA0/tiagocalof/datasets/site_a/a.csv" not in copied_remote_paths
    assert any("ln -sfn /projects/F202508843CPCAA0/tiagocalof/datasets" in cmd for cmd in fake_ssh.commands)

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    final_details = status_calls[-1]["details"]
    assert final_details["datasets_synced"] == ["datasets/site_b/b.csv"]
    assert final_details["datasets_skipped"] == ["datasets/site_a/a.csv"]


def test_deucalion_executor_dataset_directory_uses_recursive_copy(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()

    (shared_dir / "datasets" / "site_dir").mkdir(parents=True, exist_ok=True)
    (shared_dir / "datasets" / "site_dir" / "nested.csv").write_text("x", encoding="utf-8")

    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )

    _write_config(
        shared_dir,
        {
            "execution": {
                "deucalion": {
                    "sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
                    "datasets": ["datasets/site_dir"],
                }
            }
        },
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "888")
    monkeypatch.setattr(
        deucalion_executor_module,
        "query_state",
        lambda *args, **kwargs: SlurmState(state="COMPLETED", exit_code=0),
    )

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": "job-dir", "config_path": "configs/demo.yaml", "job_name": "Dir"})

    dataset_copy_calls = [
        (_local, remote, recursive)
        for _local, remote, recursive in fake_ssh.copy_to_calls
        if remote.endswith("/datasets/site_dir")
    ]
    assert dataset_copy_calls, "expected dataset directory copy_to call"
    assert dataset_copy_calls[0][2] is True


def test_deucalion_executor_stop_requested(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    session.status_responses = ["stop_requested"]
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )
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

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": "job-stop", "config_path": "configs/demo.yaml", "job_name": "Stop"})

    assert cancel_called["value"] is True
    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "stopped"


def test_deucalion_executor_unknown_timeout(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )

    _write_config(
        shared_dir,
        {"execution": {"deucalion": {"sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif"}}},
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "unknown-1")
    monkeypatch.setattr(deucalion_executor_module, "query_state", lambda *args, **kwargs: SlurmState(state="UNKNOWN"))

    now_values = iter([0.0, 0.2, 0.8, 1.3, 1.6])
    agent = _build_agent(
        shared_dir,
        session,
        fake_ssh,
        env={
            "DEUCALION_SYNC_INTERVAL": "0",
            "DEUCALION_UNKNOWN_STATE_TIMEOUT_SECONDS": "1",
        },
        now_fn=lambda: next(now_values),
    )

    agent._run_job({"job_id": "job-unknown", "config_path": "configs/demo.yaml", "job_name": "Unknown"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "failed"
    assert status_calls[-1]["error"] == "slurm_unknown_timeout"
    assert "unknown_since" in status_calls[-1]["details"]


def test_deucalion_executor_unreachable_timeout(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()
    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
        }
    )

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

    now_values = iter([0.0, 0.6, 1.2, 1.8, 2.4])
    agent = _build_agent(
        shared_dir,
        session,
        fake_ssh,
        env={
            "DEUCALION_SYNC_INTERVAL": "0",
            "DEUCALION_UNREACHABLE_GRACE_SECONDS": "1",
        },
        now_fn=lambda: next(now_values),
    )

    agent._run_job({"job_id": "job-timeout", "config_path": "configs/demo.yaml", "job_name": "Timeout"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "failed"
    assert status_calls[-1]["error"] == "deucalion_unreachable_timeout"


def test_deucalion_executor_completed_but_artifact_sync_failure_marks_failed(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()

    job_id = "job-artifact-fail"
    remote_job_dir = f"/projects/F202508843CPCAA0/tiagocalof/runs/{job_id}"
    remote_data_job_dir = f"{remote_job_dir}/data/jobs/{job_id}"
    results_path = f"{remote_data_job_dir}/results"

    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
            results_path,
            f"{remote_data_job_dir}/progress",
        },
        copy_from_failures={results_path: 3},
    )
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.out"] = "out\n"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.err"] = "err\n"

    _write_config(
        shared_dir,
        {"execution": {"deucalion": {"sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif"}}},
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "901")
    monkeypatch.setattr(
        deucalion_executor_module,
        "query_state",
        lambda *args, **kwargs: SlurmState(state="COMPLETED", exit_code=0),
    )

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": job_id, "config_path": "configs/demo.yaml", "job_name": "ArtifactsFail"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "failed"
    assert status_calls[-1]["error"] == "artifact_sync_failed"
    artifact_sync = status_calls[-1]["details"]["artifact_sync"]
    assert artifact_sync["had_failure"] is True
    assert artifact_sync["folders"]["results"]["status"] == "failed"


def test_deucalion_executor_artifact_sync_transient_failure_keeps_finished(tmp_path, monkeypatch):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    session = DummySession()

    job_id = "job-artifact-retry"
    remote_job_dir = f"/projects/F202508843CPCAA0/tiagocalof/runs/{job_id}"
    remote_data_job_dir = f"{remote_job_dir}/data/jobs/{job_id}"
    results_path = f"{remote_data_job_dir}/results"

    fake_ssh = FakeSSHClient(
        existing_paths={
            "/projects/F202508843CPCAA0/tiagocalof",
            "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif",
            results_path,
            f"{remote_data_job_dir}/progress",
        },
        copy_from_failures={results_path: 1},
    )
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.out"] = "out\n"
    fake_ssh.remote_files[f"{remote_job_dir}/slurm.err"] = "err\n"

    _write_config(
        shared_dir,
        {"execution": {"deucalion": {"sif_path": "/projects/F202508843CPCAA0/tiagocalof/images/sim.sif"}}},
    )

    monkeypatch.setattr(deucalion_executor_module, "sbatch_submit", lambda *args, **kwargs: "902")
    monkeypatch.setattr(
        deucalion_executor_module,
        "query_state",
        lambda *args, **kwargs: SlurmState(state="COMPLETED", exit_code=0),
    )

    agent = _build_agent(shared_dir, session, fake_ssh)
    agent._run_job({"job_id": job_id, "config_path": "configs/demo.yaml", "job_name": "ArtifactsRetry"})

    status_calls = [call["json"] for call in session.calls if call["url"].endswith("/job-status")]
    assert status_calls[-1]["status"] == "finished"
    artifact_sync = status_calls[-1]["details"]["artifact_sync"]
    assert artifact_sync["had_failure"] is False
    assert artifact_sync["folders"]["results"]["status"] == "synced"
