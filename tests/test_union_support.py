from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import tarfile

import pytest

from worker_agent.executors.union_executor import UnionExecutor
from worker_agent.union.archive import collect_input_paths, safe_extract
from worker_agent.union.client import (
    UnionRunSnapshot,
    _algorithms_wrapper,
    derive_object_uris,
    deterministic_run_name,
)
from worker_agent.union.config import UnionConfig
from worker_agent.union.events import encode_event, parse_event


def test_union_config_reads_headless_defaults(tmp_path: Path) -> None:
    key = tmp_path / "key"
    key.write_text("secret-key\n", encoding="utf-8")
    config = UnionConfig.from_env({"FLYTE_API_KEY_FILE": str(key)})

    assert config.endpoint == "dns:///inesctec.hosted.unionai.cloud"
    assert config.project == "humanise-energaize"
    assert config.domain == "development"
    assert config.gpu_count == 1
    assert config.unreachable_grace_seconds == 900
    assert config.retry_max_backoff_seconds == 60
    assert config.control_plane_ca_file is None
    assert config.read_api_key() == "secret-key"


def test_union_event_round_trip_ignores_invalid_lines() -> None:
    line = encode_event("progress", 3, {"progress": {"step_current": 5, "step_total": 10}})
    parsed = parse_event(f"prefix {line}")

    assert parsed is not None
    assert parsed["kind"] == "progress"
    assert parsed["sequence"] == 3
    assert parse_event("normal log line") is None
    assert parse_event("OPEVA_EVENT_V1=not-json") is None


def test_input_collection_only_includes_resolved_config_job_info_and_referenced_dataset(tmp_path: Path) -> None:
    job_id = "job-1"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    config = job_dir / "config.resolved.yaml"
    config.write_text("simulator:\n  dataset_path: /data/datasets/demo/schema.json\n", encoding="utf-8")
    (job_dir / "job_info.json").write_text("{}", encoding="utf-8")
    dataset = tmp_path / "datasets" / "demo"
    dataset.mkdir(parents=True)
    (dataset / "schema.json").write_text("{}", encoding="utf-8")
    unrelated = tmp_path / "datasets" / "other"
    unrelated.mkdir(parents=True)

    paths = collect_input_paths(tmp_path, job_id, f"jobs/{job_id}/config.resolved.yaml")

    assert {path.resolve() for path in paths} == {config.resolve(), (job_dir / "job_info.json").resolve(), dataset.resolve()}


def test_safe_extract_rejects_path_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "bad.tar.gz"
    source = tmp_path / "payload"
    source.write_text("bad", encoding="utf-8")
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source, arcname="../escape")

    with pytest.raises(ValueError, match="Unsafe path"):
        safe_extract(archive_path, tmp_path / "out")


def test_union_run_helpers_are_deterministic() -> None:
    input_uri = "s3://bucket/path/input.tar.gz"
    assert derive_object_uris(input_uri) == (
        "s3://bucket/path/result.tar.gz",
        "s3://bucket/path/cancel.request",
    )
    assert deterministic_run_name("D520A2AB-97DC-48BD-BD35-123456789012", 2) == (
        "opeva-d520a2ab97dc48bdbd35123456789012-a2"
    )


def test_algorithms_wrapper_marks_process_started_after_launch() -> None:
    wrapper = _algorithms_wrapper("job-1", "--config /data/config.yaml --job_id job-1", 60)

    assert wrapper.index("child=$!") < wrapper.index("algorithm.started")


class FakeRuntime:
    def __init__(self, shared_dir: Path) -> None:
        self.worker_id = "union-inesctec"
        self.image = "calof/opeva_simulator:latest"
        self.status_poll_interval = 0.01
        self.shared_dir = str(shared_dir)
        self.statuses: list[tuple[str, str, dict]] = []
        self.active: dict[str, dict] = {}
        self.backend_status = "running"
        self.bound_attempts: dict[str, dict] = {}

    def _bind_job_attempt(self, job: dict) -> None:
        self.bound_attempts[str(job["job_id"])] = dict(job)

    def _post_status(self, job_id: str, status: str, **extra: object) -> bool:
        self.statuses.append((job_id, status, dict(extra)))
        return True

    def _fetch_status(self, job_id: str) -> str:
        return self.backend_status

    def _send_heartbeat(self, force: bool = False) -> None:
        return None

    def _register_active_job(self, job_id: str, job_name: str | None = None) -> None:
        self.active[job_id] = {"job_name": job_name}

    def _unregister_active_job(self, job_id: str) -> None:
        self.active.pop(job_id, None)

    def _update_active_job(self, job_id: str, **fields: object) -> None:
        self.active.setdefault(job_id, {}).update(fields)

    def _prepare_log_file(self, job_id: str) -> Path:
        path = Path(self.shared_dir) / "jobs" / job_id / "logs" / f"{job_id}.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        return path

    def _build_command(self, job_id: str, config_path: str) -> str:
        return f"--config /data/{config_path} --job_id {job_id}"

    def _build_container_name(self, job_id: str, job_name: str) -> str:
        return job_id

    def _build_volumes(self, vols):
        return {}

    def _build_device_requests(self, job=None):
        return None

    def _mark_active_job(self, job_id):
        return None


class FakeUnionClient:
    def __init__(self, config: UnionConfig, result_archive: Path) -> None:
        self.config = config
        self.result_archive = result_archive
        self.deleted = False
        self.submitted: dict | None = None
        self.submit_calls = 0
        self.aborted_runs: list[str] = []
        self.artifact = {
            "uri": "s3://bucket/run/result.tar.gz",
            "size": result_archive.stat().st_size,
            "sha256": hashlib.sha256(result_archive.read_bytes()).hexdigest(),
            "get_url": "https://objects.invalid/get",
            "delete_url": "https://objects.invalid/delete",
        }

    def initialize(self) -> None:
        return None

    def upload_input(self, path: Path, job_id: str, attempt: int) -> str:
        assert path.is_file()
        return "s3://bucket/run/input.tar.gz"

    def submit_job(self, **kwargs) -> UnionRunSnapshot:
        self.submit_calls += 1
        self.submitted = kwargs
        return UnionRunSnapshot(name=kwargs["run_name"], phase="QUEUED", url="https://union/run")

    def get_run(self, run_name: str) -> UnionRunSnapshot:
        return UnionRunSnapshot(name=run_name, phase="SUCCEEDED", url="https://union/run")

    def stream_logs(self, run_name: str):
        yield encode_event("setup", 1, {"phase": "downloading_input", "cancel_put_url": "https://cancel"})
        yield encode_event("started", 2, {"started_at": 1000.0})
        yield "[algorithms] training line\n"
        yield encode_event("progress", 3, {"progress": {"step_current": 10, "step_total": 10, "progress_pct": 100}})
        yield encode_event("artifact", 4, {"artifact": self.artifact})
        yield encode_event("terminal", 5, {"status": "finished", "exit_code": 0})

    def refresh_artifact(self, result_uri: str, job_id: str) -> dict:
        return dict(self.artifact)

    def download_artifact(self, get_url: str, destination: Path) -> None:
        shutil.copy2(self.result_archive, destination)

    def delete_artifact(self, delete_url: str) -> None:
        self.deleted = True

    def request_graceful_cancel(self, put_url: str) -> None:
        return None

    def abort(self, run_name: str, reason: str) -> None:
        self.aborted_runs.append(run_name)


def _make_result_archive(root: Path, job_id: str) -> Path:
    payload = root / "result-payload" / "jobs" / job_id
    (payload / "logs").mkdir(parents=True)
    (payload / "progress").mkdir(parents=True)
    (payload / "results").mkdir(parents=True)
    (payload / "logs" / f"{job_id}.log").write_text("training line\n", encoding="utf-8")
    (payload / "progress" / "progress.json").write_text('{"progress_pct": 100}', encoding="utf-8")
    (payload / "results" / "result.json").write_text('{"status": "completed"}', encoding="utf-8")
    archive = root / "result.tar.gz"
    with tarfile.open(archive, "w:gz") as handle:
        handle.add(payload, arcname=f"jobs/{job_id}")
    return archive


def test_union_executor_completes_fake_remote_run_and_installs_results(tmp_path: Path) -> None:
    job_id = "d520a2ab-97dc-48bd-bd35-123456789012"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    config_path = job_dir / "config.resolved.yaml"
    config_path.write_text("simulator: {}\n", encoding="utf-8")
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}), encoding="utf-8")
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    fake_holder: dict[str, FakeUnionClient] = {}

    def factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        fake_holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )
    executor.run_job(
        {
            "job_id": job_id,
            "job_name": "union-smoke",
            "config_path": f"jobs/{job_id}/config.resolved.yaml",
            "image": "calof/opeva_simulator:sha-test",
            "command": f"--config /data/jobs/{job_id}/config.resolved.yaml --job_id {job_id}",
            "attempt_number": 1,
            "env": {},
        }
    )

    assert fake_holder["client"].submitted is not None
    assert fake_holder["client"].deleted is True
    assert json.loads((job_dir / "results" / "result.json").read_text())["status"] == "completed"
    assert json.loads((job_dir / "progress" / "progress.json").read_text())["progress_pct"] == 100
    state = json.loads((job_dir / ".worker" / "union.json").read_text())
    assert state["terminal"] is True
    assert state["terminal_status"] == "finished"
    assert state["orchestrator_ack"] is True
    assert state["submitted"] is True
    assert state["union_run_id"] == deterministic_run_name(job_id, 1)
    assert [status for _, status, _ in runtime.statuses][-1] == "finished"
    running = next(extra for _, status, extra in runtime.statuses if status == "running")
    assert running["details"]["started_at"] == 1000.0


def test_union_executor_aborts_superseded_attempt_before_starting_next(tmp_path: Path) -> None:
    job_id = "6496b47d-6cf6-48bd-bd35-123456789012"
    job_dir = tmp_path / "jobs" / job_id
    state_dir = job_dir / ".worker"
    state_dir.mkdir(parents=True)
    config_path = job_dir / "config.resolved.yaml"
    config_path.write_text("simulator: {}\n", encoding="utf-8")
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}), encoding="utf-8")
    old_run_name = deterministic_run_name(job_id, 1)
    (state_dir / "union.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "job_id": job_id,
                "job_name": "old-attempt",
                "worker_id": "union-inesctec",
                "attempt": 1,
                "run_name": old_run_name,
                "submitted": True,
                "terminal": False,
                "orchestrator_ack": False,
            }
        ),
        encoding="utf-8",
    )
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    holder: dict[str, FakeUnionClient] = {}

    def factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )
    executor.run_job(
        {
            "job_id": job_id,
            "job_name": "new-attempt",
            "config_path": f"jobs/{job_id}/config.resolved.yaml",
            "image": "calof/opeva_simulator:sha-test",
            "attempt_number": 2,
            "attempt_token": "new-attempt-token",
            "env": {},
        }
    )

    assert holder["client"].aborted_runs == [old_run_name]
    state = json.loads((state_dir / "union.json").read_text())
    assert state["attempt"] == 2
    assert state["run_name"] == deterministic_run_name(job_id, 2)
    assert state["terminal_status"] == "finished"


def test_union_executor_recovers_restart_between_upload_and_submit(tmp_path: Path) -> None:
    job_id = "5b633395-8b18-441e-a9f0-7d9375b01330"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    fake_holder: dict[str, FakeUnionClient] = {}

    def factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        fake_holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )
    run_name = deterministic_run_name(job_id, 2)
    state = {
        "schema_version": 1,
        "job_id": job_id,
        "job_name": "recover-submit",
        "attempt": 2,
        "run_name": run_name,
        "union_run_id": run_name,
        "input_uri": "s3://bucket/run/input.tar.gz",
        "result_uri": "s3://bucket/run/result.tar.gz",
        "cancel_uri": "s3://bucket/run/cancel.request",
        "terminal": False,
        "submitted": False,
        "last_event_sequence": 0,
        "log_line_count": 0,
        "algorithm_lines_relayed": 0,
        "job_payload": {
            "job_id": job_id,
            "job_name": "recover-submit",
            "config_path": f"jobs/{job_id}/config.resolved.yaml",
            "image": "calof/opeva_simulator:sha-test",
            "command": f"--config /data/jobs/{job_id}/config.resolved.yaml --job_id {job_id}",
            "attempt_number": 2,
            "env": {},
        },
    }

    executor._resume_state(state)

    assert fake_holder["client"].submitted is not None
    assert fake_holder["client"].submitted["run_name"] == run_name
    persisted = json.loads((job_dir / ".worker" / "union.json").read_text())
    assert persisted["submitted"] is True
    assert persisted["terminal_status"] == "finished"


def test_union_executor_recovers_submitted_run_without_duplicate_submission(tmp_path: Path) -> None:
    job_id = "a038d55d-8536-4a54-a937-d5931e8bfd53"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    fake_holder: dict[str, FakeUnionClient] = {}

    def factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        fake_holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )
    run_name = deterministic_run_name(job_id, 1)
    state = {
        "schema_version": 1,
        "job_id": job_id,
        "job_name": "recover-running",
        "attempt": 1,
        "run_name": run_name,
        "union_run_id": run_name,
        "input_uri": "s3://bucket/run/input.tar.gz",
        "result_uri": "s3://bucket/run/result.tar.gz",
        "cancel_uri": "s3://bucket/run/cancel.request",
        "terminal": False,
        "submitted": True,
        "last_event_sequence": 0,
        "log_line_count": 0,
        "algorithm_lines_relayed": 0,
    }

    executor._resume_state(state)

    assert fake_holder["client"].submit_calls == 0
    persisted = json.loads((job_dir / ".worker" / "union.json").read_text())
    assert persisted["terminal_status"] == "finished"


def test_union_executor_refreshes_expired_artifact_urls(tmp_path: Path) -> None:
    job_id = "b875d391-0876-4747-8830-3aa4b9d69a12"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)

    class ExpiredUrlClient(FakeUnionClient):
        def __init__(self, config: UnionConfig, archive: Path) -> None:
            super().__init__(config, archive)
            self.download_calls = 0
            self.delete_calls = 0
            self.refresh_calls = 0

        def refresh_artifact(self, result_uri: str, job_id: str) -> dict:
            self.refresh_calls += 1
            return dict(self.artifact)

        def download_artifact(self, get_url: str, destination: Path) -> None:
            self.download_calls += 1
            if self.download_calls == 1:
                raise RuntimeError("expired download URL")
            super().download_artifact(get_url, destination)

        def delete_artifact(self, delete_url: str) -> None:
            self.delete_calls += 1
            if self.delete_calls == 1:
                raise RuntimeError("expired delete URL")
            super().delete_artifact(delete_url)

    client_holder: dict[str, ExpiredUrlClient] = {}

    def factory(config: UnionConfig) -> ExpiredUrlClient:
        client = ExpiredUrlClient(config, result_archive)
        client_holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )
    state = {
        "job_id": job_id,
        "result_uri": "s3://bucket/run/result.tar.gz",
        "algorithm_lines_relayed": 0,
    }
    log_path = runtime._prepare_log_file(job_id)

    executor._install_artifact(state, dict(client_holder["client"].artifact), log_path)

    client = client_holder["client"]
    assert client.download_calls == 2
    assert client.delete_calls == 2
    assert client.refresh_calls == 2
    assert client.deleted is True
    assert state["artifact_installed"] is True
    assert state["artifact_deleted"] is True
    assert json.loads((job_dir / "results" / "result.json").read_text())["status"] == "completed"


def test_union_executor_retries_transient_control_plane_failure(tmp_path: Path) -> None:
    job_id = "1dc2ffaf-268d-4102-8e60-69ff249f020f"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    (job_dir / "config.resolved.yaml").write_text("simulator: {}\n", encoding="utf-8")
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}), encoding="utf-8")
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)

    class TransientClient(FakeUnionClient):
        def __init__(self, config: UnionConfig, archive: Path) -> None:
            super().__init__(config, archive)
            self.get_run_calls = 0

        def get_run(self, run_name: str) -> UnionRunSnapshot:
            self.get_run_calls += 1
            if self.get_run_calls <= 2:
                raise RuntimeError("temporary Union outage")
            return super().get_run(run_name)

    holder: dict[str, TransientClient] = {}

    def factory(config: UnionConfig) -> TransientClient:
        client = TransientClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={
            "FLYTE_API_KEY_FILE": str(tmp_path / "unused"),
            "UNION_POLL_INTERVAL_SECONDS": "1",
            "UNION_RETRY_MAX_BACKOFF_SECONDS": "2",
        },
        client_factory=factory,
        wait_fn=lambda _seconds: False,
    )
    executor.run_job(
        {
            "job_id": job_id,
            "job_name": "transient-control-plane",
            "config_path": f"jobs/{job_id}/config.resolved.yaml",
            "image": "calof/opeva_simulator:sha-test",
            "command": f"--config /data/jobs/{job_id}/config.resolved.yaml --job_id {job_id}",
            "attempt_number": 1,
            "env": {},
        }
    )

    assert holder["client"].get_run_calls == 3
    state = json.loads((job_dir / ".worker" / "union.json").read_text())
    assert state["terminal_status"] == "finished"
    assert state["orchestrator_ack"] is True
    assert "control_plane_unreachable_since" not in state
    assert any(
        extra.get("details", {}).get("connectivity") == "degraded"
        for _, _, extra in runtime.statuses
    )


def test_union_executor_replays_unacknowledged_terminal_state_on_startup(tmp_path: Path) -> None:
    job_id = "8d783bbb-971e-40d3-a6b2-56816416f0a3"
    job_dir = tmp_path / "jobs" / job_id
    state_dir = job_dir / ".worker"
    state_dir.mkdir(parents=True)
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    runtime.backend_status = "dispatched"
    holder: dict[str, FakeUnionClient] = {}

    def factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        holder["client"] = client
        return client

    state = {
        "schema_version": 2,
        "job_id": job_id,
        "job_name": "terminal-replay",
        "attempt": 1,
        "run_name": deterministic_run_name(job_id, 1),
        "terminal": True,
        "terminal_status": "finished",
        "terminal_stage": "union:finished",
        "orchestrator_ack": False,
        "job_payload": {
            "job_id": job_id,
            "attempt_number": 2,
            "attempt_token": "union-recovery-token",
        },
    }
    (state_dir / "union.json").write_text(json.dumps(state), encoding="utf-8")
    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )

    executor.on_startup()
    for thread in list(executor._recovery_threads.values()):
        thread.join(timeout=2)

    persisted = json.loads((state_dir / "union.json").read_text())
    assert persisted["orchestrator_ack"] is True
    assert holder["client"].submit_calls == 0
    assert runtime.bound_attempts[job_id]["attempt_token"] == "union-recovery-token"
    assert [status for _, status, _ in runtime.statuses][-2:] == ["running", "finished"]


def test_union_startup_defers_recovery_while_job_is_queued(tmp_path: Path) -> None:
    job_id = "bc1a0e08-b705-4c3b-a60f-1bfe93cda831"
    state_dir = tmp_path / "jobs" / job_id / ".worker"
    state_dir.mkdir(parents=True)
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    runtime.backend_status = "queued"
    holder: dict[str, FakeUnionClient] = {}

    def factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        holder["client"] = client
        return client

    (state_dir / "union.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "job_id": job_id,
                "worker_id": "union-inesctec",
                "run_name": deterministic_run_name(job_id, 1),
                "terminal": False,
                "orchestrator_ack": False,
            }
        ),
        encoding="utf-8",
    )
    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )

    executor.on_startup()

    assert executor._recovery_threads == {}
    assert runtime.active == {}
    assert holder["client"].submit_calls == 0


def test_union_terminal_state_remains_recoverable_until_status_is_acknowledged(tmp_path: Path) -> None:
    job_id = "d4bbcb4a-f177-4435-b744-ad87eed93069"
    result_archive = _make_result_archive(tmp_path, job_id)

    class ToggleAckRuntime(FakeRuntime):
        acknowledge = False

        def _post_status(self, job_id: str, status: str, **extra: object) -> bool:
            super()._post_status(job_id, status, **extra)
            return self.acknowledge

    runtime = ToggleAckRuntime(tmp_path)
    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=lambda config: FakeUnionClient(config, result_archive),
    )
    state = {
        "schema_version": 2,
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "union_phase": "SUCCEEDED",
    }

    executor._finalize_terminal(state, "finished")

    persisted = json.loads((tmp_path / "jobs" / job_id / ".worker" / "union.json").read_text())
    assert persisted["terminal"] is True
    assert persisted["orchestrator_ack"] is False

    runtime.acknowledge = True
    assert executor._replay_terminal_status(state) is True
    persisted = json.loads((tmp_path / "jobs" / job_id / ".worker" / "union.json").read_text())
    assert persisted["orchestrator_ack"] is True


def test_union_artifact_cleanup_resumes_without_reinstalling_results(tmp_path: Path) -> None:
    job_id = "b70e331a-e34f-46c4-827f-0b45539307bd"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)

    class CleanupOnlyClient(FakeUnionClient):
        def __init__(self, config: UnionConfig, archive: Path) -> None:
            super().__init__(config, archive)
            self.download_calls = 0

        def download_artifact(self, get_url: str, destination: Path) -> None:
            self.download_calls += 1
            raise AssertionError("an installed artifact must not be downloaded again")

        def delete_artifact(self, delete_url: str) -> None:
            persisted = json.loads((job_dir / ".worker" / "union.json").read_text())
            assert persisted["artifact_installed"] is True
            super().delete_artifact(delete_url)

    holder: dict[str, CleanupOnlyClient] = {}

    def factory(config: UnionConfig) -> CleanupOnlyClient:
        client = CleanupOnlyClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=factory,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "result_uri": "s3://bucket/run/result.tar.gz",
        "artifact": dict(holder["client"].artifact),
        "artifact_installed": True,
        "artifact_deleted": False,
        "algorithm_lines_relayed": 0,
    }
    executor._save_state(state)

    executor._install_artifact(state, dict(holder["client"].artifact), runtime._prepare_log_file(job_id))

    assert holder["client"].download_calls == 0
    assert holder["client"].deleted is True
    persisted = json.loads((job_dir / ".worker" / "union.json").read_text())
    assert persisted["artifact_installed"] is True
    assert persisted["artifact_deleted"] is True
