from __future__ import annotations

import errno
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tarfile
import threading
import time
from types import ModuleType, SimpleNamespace

import pytest

from worker_agent.executors.union_executor import UnionExecutor, _gpu_model_from_log
from worker_agent.union.archive import (
    append_missing_algorithm_logs,
    collect_input_paths,
    measure_job_storage,
    merge_job_results,
    safe_extract,
    write_result_storage_manifest,
)
from worker_agent.union.client import (
    FlyteUnionClient,
    UnionRunSnapshot,
    _algorithms_wrapper,
    artifact_signer_run_name,
    derive_object_uris,
    deterministic_run_name,
)
from worker_agent.union.config import UnionConfig
from worker_agent.union.events import encode_event, parse_event
from worker_agent.union.runner import _cancel_object_exists, _read_gpu_model, _retry_store_operation


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
    assert config.auth_verify_interval_seconds == 300
    assert config.max_concurrent_recoveries == 1
    assert config.recovery_min_free_gib == 20
    assert config.recovery_unknown_size_multiplier == 4
    assert config.signer_log_grace_seconds == 30
    assert config.control_plane_ca_file is None
    assert config.read_api_key() == "secret-key"


def test_union_config_supports_device_flow_without_api_key() -> None:
    config = UnionConfig.from_env({"UNION_AUTH_MODE": "device_flow"})

    assert config.auth_mode == "device_flow"
    assert config.api_key_file is None
    with pytest.raises(RuntimeError, match="device-flow"):
        config.read_api_key()


def test_union_executor_blocks_new_jobs_until_device_authentication(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path)

    class AuthClient:
        def __init__(self, _config):
            self.state = {"status": "authentication_required", "user_code": "ABCD"}
            self.started = 0

        def auth_state(self):
            return dict(self.state)

        def start_device_authentication(self, _request_id=None):
            self.started += 1
            return True

    executor = UnionExecutor(
        runtime,
        env={"UNION_AUTH_MODE": "device_flow"},
        client_factory=AuthClient,
    )

    assert executor.ready_for_new_jobs() is False
    assert executor.heartbeat_info()["union_auth"]["user_code"] == "ABCD"
    executor.handle_command({"action": "union_authenticate", "request_id": "request-1"})
    assert executor.client.started == 1
    executor.client.state = {"status": "authenticated"}
    assert executor.ready_for_new_jobs() is True


def test_device_auth_state_reopens_and_recovers_during_active_calls(monkeypatch) -> None:
    # The base test extra intentionally excludes the optional Flyte SDK. Hook
    # installation is covered by the Union image smoke test; this unit test only
    # exercises the worker's auth state transitions.
    monkeypatch.setattr("worker_agent.union.client._install_device_auth_hooks", lambda _client: None)
    client = FlyteUnionClient(UnionConfig.from_env({"UNION_AUTH_MODE": "device_flow"}))

    class DeviceResponse:
        verification_uri = "https://signin.example/activate"
        user_code = "ABCD-EFGH"
        expires_in = 600

    client._initialized = True
    client._set_auth_state("checking", request_id="request-1")
    client._device_authorization_required(DeviceResponse())

    required = client.auth_state()
    assert required["status"] == "authentication_required"
    assert required["request_id"] == "request-1"
    assert required["verification_url_complete"].endswith("user_code=ABCD-EFGH")

    client._device_authorization_completed()
    assert client.auth_state()["status"] == "authenticated"


def test_periodic_auth_verification_reuses_initialized_client(monkeypatch) -> None:
    monkeypatch.setattr("worker_agent.union.client._install_device_auth_hooks", lambda _client: None)
    client = FlyteUnionClient(UnionConfig.from_env({"UNION_AUTH_MODE": "device_flow"}))
    client._initialized = True
    client._set_auth_state("authenticated", last_verified_at=-100)
    calls = 0

    class Run:
        @staticmethod
        def listall(*, limit: int):
            nonlocal calls
            assert limit == 1
            calls += 1
            return iter(())

    flyte_module = ModuleType("flyte")
    flyte_module.__path__ = []
    remote_module = ModuleType("flyte.remote")
    remote_module.Run = Run
    monkeypatch.setitem(sys.modules, "flyte", flyte_module)
    monkeypatch.setitem(sys.modules, "flyte.remote", remote_module)

    client.ensure_authentication_fresh(1)
    assert client._auth_thread is not None
    client._auth_thread.join(timeout=2)

    assert calls == 1
    assert client.auth_state()["status"] == "authenticated"
    assert client.auth_state()["last_verified_at"] > 0


def test_periodic_auth_verification_does_not_invalidate_on_network_error(monkeypatch) -> None:
    monkeypatch.setattr("worker_agent.union.client._install_device_auth_hooks", lambda _client: None)
    client = FlyteUnionClient(UnionConfig.from_env({"UNION_AUTH_MODE": "device_flow"}))
    client._initialized = True
    client._set_auth_state("authenticated", last_verified_at=-100)

    class Run:
        @staticmethod
        def listall(*, limit: int):
            assert limit == 1
            raise RuntimeError("connection reset by peer")

    flyte_module = ModuleType("flyte")
    flyte_module.__path__ = []
    remote_module = ModuleType("flyte.remote")
    remote_module.Run = Run
    monkeypatch.setitem(sys.modules, "flyte", flyte_module)
    monkeypatch.setitem(sys.modules, "flyte.remote", remote_module)

    client.ensure_authentication_fresh(1)
    assert client._auth_thread is not None
    client._auth_thread.join(timeout=2)

    state = client.auth_state()
    assert state["status"] == "authenticated"
    assert state["verification_error"] == "connection reset by peer"
    assert client._initialized is True


def test_union_container_task_runs_without_rebundling_baked_image(monkeypatch) -> None:
    monkeypatch.setattr("worker_agent.union.client._install_device_auth_hooks", lambda _client: None)
    client = FlyteUnionClient(UnionConfig.from_env({"UNION_AUTH_MODE": "device_flow"}))
    client._initialized = True
    task = object()
    monkeypatch.setattr(client, "_pod_task", lambda **_kwargs: task)
    captured: dict[str, object] = {}

    class RunContext:
        def run(self, submitted_task):
            assert submitted_task is task
            return SimpleNamespace(name="run-1", phase="queued", url="https://union/run-1")

    def with_runcontext(**kwargs):
        captured.update(kwargs)
        return RunContext()

    monkeypatch.setitem(sys.modules, "flyte", SimpleNamespace(with_runcontext=with_runcontext))

    snapshot = client.submit_job(
        job={"job_id": "job-1"},
        run_name="run-1",
        input_uri="s3://bucket/input",
        result_uri="s3://bucket/result",
        cancel_uri="s3://bucket/cancel",
    )

    assert captured["copy_style"] == "none"
    assert captured["version"] == "run-1"
    assert snapshot.name == "run-1"


def test_union_submission_adopts_run_after_response_is_lost(monkeypatch) -> None:
    monkeypatch.setattr("worker_agent.union.client._install_device_auth_hooks", lambda _client: None)
    client = FlyteUnionClient(UnionConfig.from_env({"UNION_AUTH_MODE": "device_flow"}))
    client._initialized = True
    monkeypatch.setattr(client, "_pod_task", lambda **_kwargs: object())
    recovered = UnionRunSnapshot(name="run-1", phase="RUNNING", url="https://union/run-1")
    monkeypatch.setattr(client, "get_run", lambda _name: recovered)

    class LostResponseContext:
        def run(self, _task):
            raise RuntimeError("connection reset after Union accepted the run")

    monkeypatch.setitem(
        sys.modules,
        "flyte",
        SimpleNamespace(with_runcontext=lambda **_kwargs: LostResponseContext()),
    )

    snapshot = client.submit_job(
        job={"job_id": "job-1"},
        run_name="run-1",
        input_uri="s3://bucket/input",
        result_uri="s3://bucket/result",
        cancel_uri="s3://bucket/cancel",
    )

    assert snapshot is recovered


def test_union_event_round_trip_ignores_invalid_lines() -> None:
    line = encode_event("progress", 3, {"progress": {"step_current": 5, "step_total": 10}})
    parsed = parse_event(f"prefix {line}")

    assert parsed is not None
    assert parsed["kind"] == "progress"
    assert parsed["sequence"] == 3
    assert parse_event("normal log line") is None
    assert parse_event("OPEVA_EVENT_V1=not-json") is None


def test_union_artifact_signer_reads_event_while_run_is_active(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("worker_agent.union.client._install_device_auth_hooks", lambda _client: None)
    client = FlyteUnionClient(UnionConfig.from_env({"UNION_AUTH_MODE": "device_flow"}))
    client._initialized = True
    artifact = {
        "uri": "s3://bucket/result.tar.gz",
        "size": 42,
        "sha256": "a" * 64,
        "get_url": "https://objects.invalid/get",
        "delete_url": "https://objects.invalid/delete",
    }

    class ActiveRun:
        name = "signer-run"
        phase = "ActionPhase.RUNNING"

        def get_logs(self, **_kwargs):
            yield encode_event("artifact", 1, {"artifact": artifact})

    active_run = ActiveRun()

    class RunType:
        @classmethod
        def get(cls, name: str):
            assert name == "signer-run"
            return active_run

    class RunContext:
        def run(self, _task):
            return active_run

    class Value:
        def __init__(self, **kwargs):
            self.values = kwargs

    fake_flyte = SimpleNamespace(
        Resources=Value,
        PodTemplate=SimpleNamespace(from_spec=lambda *_args, **_kwargs: object()),
        TaskEnvironment=SimpleNamespace(from_task=lambda *_args, **_kwargs: None),
        with_runcontext=lambda **_kwargs: RunContext(),
    )
    monkeypatch.setitem(sys.modules, "flyte", fake_flyte)
    monkeypatch.setitem(sys.modules, "flyte.extras", SimpleNamespace(ContainerTask=Value))
    monkeypatch.setitem(sys.modules, "flyte.remote", SimpleNamespace(Run=RunType))
    monkeypatch.setitem(
        sys.modules,
        "kubernetes.client",
        SimpleNamespace(V1Container=Value, V1EnvVar=Value, V1PodSpec=Value),
    )
    monkeypatch.setattr(
        "worker_agent.union.client.artifact_signer_run_name",
        lambda _job_id: "signer-run",
    )

    assert client.refresh_artifact("s3://bucket/result.tar.gz", "job-1") == artifact


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


def test_result_storage_manifest_breaks_down_installed_job_files(tmp_path: Path) -> None:
    job_dir = tmp_path / "jobs" / "job-1"
    files = {
        "results/exported_kpis.csv": b"k" * 11,
        "results/exported_building_1.csv": b"t" * 17,
        "checkpoints/latest_checkpoint.pth": b"c" * 23,
        "logs/job-1.log": b"l" * 7,
        "result.json": b"o" * 5,
        ".worker/union-result.tar.gz": b"x" * 101,
    }
    for relative, content in files.items():
        path = job_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    measured = measure_job_storage(job_dir)
    manifest = write_result_storage_manifest(
        job_dir,
        transferred_bytes=29,
        announced_unpacked_bytes=63,
        announced_file_count=5,
    )

    assert measured["bytes"] == 63
    assert measured["file_count"] == 5
    assert measured["categories"] == {
        "kpis": {"bytes": 11, "file_count": 1},
        "timeseries": {"bytes": 17, "file_count": 1},
        "checkpoints": {"bytes": 23, "file_count": 1},
        "logs": {"bytes": 7, "file_count": 1},
        "other": {"bytes": 5, "file_count": 1},
    }
    assert manifest["transfer"]["bytes"] == 29
    assert manifest["installed"] == measured
    persisted = json.loads((job_dir / ".worker" / "result-storage.json").read_text())
    assert persisted == manifest


def test_union_run_helpers_are_deterministic() -> None:
    input_uri = "s3://bucket/path/input.tar.gz"
    assert derive_object_uris(input_uri) == (
        "s3://bucket/path/result.tar.gz",
        "s3://bucket/path/cancel.request",
    )
    run_name = deterministic_run_name("D520A2AB-97DC-48BD-BD35-123456789012", 2)
    assert run_name == deterministic_run_name("D520A2AB-97DC-48BD-BD35-123456789012", 2)
    assert run_name != deterministic_run_name("D520A2AB-97DC-48BD-BD35-123456789012", 3)
    assert len(run_name) == 30

    signer_name = artifact_signer_run_name("D520A2AB-97DC-48BD-BD35-123456789012", nonce=1)
    assert signer_name != artifact_signer_run_name("D520A2AB-97DC-48BD-BD35-123456789012", nonce=2)
    assert len(signer_name) == 30


def test_algorithms_wrapper_marks_process_started_after_launch() -> None:
    wrapper = _algorithms_wrapper("job-1", "--config /data/config.yaml --job_id job-1", 60)

    assert "nvidia-smi --query-gpu=name" in wrapper
    assert wrapper.index("gpu.model") < wrapper.index("child=$!")
    assert wrapper.index("child=$!") < wrapper.index("algorithm.started")


def test_read_gpu_model_normalizes_the_once_per_job_marker(tmp_path: Path) -> None:
    marker = tmp_path / "gpu.model"
    marker.write_text("  NVIDIA RTX PRO 6000 Blackwell\nServer Edition  \n", encoding="utf-8")

    assert _read_gpu_model(marker) == "NVIDIA RTX PRO 6000 Blackwell Server Edition"
    assert _read_gpu_model(tmp_path / "missing") is None


def test_gpu_model_can_be_recovered_from_existing_algorithm_logs(tmp_path: Path) -> None:
    log_path = tmp_path / "job.log"
    log_path.write_text(
        "startup\nCUDA device selected: NVIDIA H200\ntraining\n",
        encoding="utf-8",
    )

    assert _gpu_model_from_log(log_path) == "NVIDIA H200"


def test_append_missing_algorithm_logs_streams_only_the_unrelayed_tail(tmp_path: Path) -> None:
    job_id = "job-long-log"
    remote_log = tmp_path / "extracted" / "jobs" / job_id / "logs" / f"{job_id}.log"
    remote_log.parent.mkdir(parents=True)
    remote_log.write_text("".join(f"line-{index}\n" for index in range(50_000)), encoding="utf-8")
    local_log = tmp_path / "local.log"
    local_log.write_text("existing\n", encoding="utf-8")

    total = append_missing_algorithm_logs(local_log, tmp_path / "extracted", job_id, 49_995)

    assert total == 50_000
    assert local_log.read_text(encoding="utf-8").splitlines() == [
        "existing",
        "line-49995",
        "line-49996",
        "line-49997",
        "line-49998",
        "line-49999",
    ]


def test_runner_retries_critical_object_store_operations() -> None:
    calls = 0
    sleeps: list[float] = []

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("temporary object store outage")
        return "ok"

    assert _retry_store_operation("test", operation, attempts=4, sleep_fn=sleeps.append) == "ok"
    assert calls == 3
    assert sleeps == [1, 2]


def test_runner_cancel_probe_failure_does_not_fail_training(capsys) -> None:
    class UnreachableStore:
        def exists(self, _uri: str) -> bool:
            raise RuntimeError("temporary object store outage")

    assert _cancel_object_exists(UnreachableStore(), "s3://bucket/cancel") is False
    assert "training continues" in capsys.readouterr().err


class FakeRuntime:
    def __init__(self, shared_dir: Path) -> None:
        self.worker_id = "union-inesctec"
        self.image = "calof/opeva_simulator:latest"
        self.status_poll_interval = 0.01
        self.shared_dir = str(shared_dir)
        self.statuses: list[tuple[str, str, dict]] = []
        self.active: dict[str, dict] = {}
        self.backend_status = "running"
        self.backend_exists: bool | None = True
        self.bound_attempts: dict[str, dict] = {}

    def _bind_job_attempt(self, job: dict) -> None:
        self.bound_attempts[str(job["job_id"])] = dict(job)

    def _post_status(self, job_id: str, status: str, **extra: object) -> bool:
        self.statuses.append((job_id, status, dict(extra)))
        return True

    def _fetch_status(self, job_id: str) -> str:
        return self.backend_status

    def _fetch_status_with_presence(self, job_id: str) -> tuple[str | None, bool | None]:
        return self.backend_status, self.backend_exists

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
        yield encode_event("started", 2, {"started_at": 1000.0, "gpu_model": "NVIDIA H200"})
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


def test_union_progress_recovers_missing_started_event_and_final_replay_corrects_it(
    tmp_path: Path,
) -> None:
    job_id = "job-missed-started"
    runtime = FakeRuntime(tmp_path)

    class ReplayClient:
        def stream_logs(self, _run_name: str):
            yield encode_event(
                "started",
                2,
                {"started_at": 1000.0, "gpu_model": "NVIDIA RTX PRO 6000 Blackwell"},
            )
            yield encode_event(
                "artifact",
                6,
                {
                    "artifact": {
                        "uri": "s3://bucket/result.tar.gz",
                        "get_url": "https://objects.invalid/get",
                        "delete_url": "https://objects.invalid/delete",
                    }
                },
            )
            yield encode_event("terminal", 7, {"status": "finished", "exit_code": 0})

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=lambda _config: ReplayClient(),
    )
    state = {
        "job_id": job_id,
        "run_name": "run-missed-started",
        "last_event_sequence": 4,
        "log_line_count": 5,
    }
    log_path = runtime._prepare_log_file(job_id)

    executor._handle_log_line(
        state,
        log_path,
        encode_event("progress", 5, {"timestamp": 1100.0, "progress": {"progress_pct": 25}}),
    )
    assert state["started_at"] == 1100.0
    assert state["started_at_source"] == "progress"
    assert runtime.statuses[-1][1] == "running"

    executor._replay_final_events(state, log_path)

    assert state["started_at"] == 1000.0
    assert state["started_at_source"] == "event"
    assert state["gpu_model"] == "NVIDIA RTX PRO 6000 Blackwell"
    assert state["artifact"]["uri"] == "s3://bucket/result.tar.gz"
    assert state["runner_terminal_status"] == "finished"
    assert state["runner_exit_code"] == 0


def test_union_log_pump_reconnects_and_resumes_from_persisted_cursor(tmp_path: Path) -> None:
    job_id = "79246b8e-375d-4ca4-b863-2e18ba6339aa"
    runtime = FakeRuntime(tmp_path)

    class ReconnectingClient:
        def __init__(self, _config: UnionConfig) -> None:
            self.calls = 0

        def stream_logs(self, _run_name: str):
            self.calls += 1
            yield encode_event("started", 1, {"started_at": 1000.0})
            if self.calls == 1:
                raise RuntimeError("incomplete envelope: unexpected EOF")
            yield encode_event(
                "progress",
                2,
                {"progress": {"step_current": 5, "step_total": 10, "progress_pct": 50}},
            )

    executor = UnionExecutor(
        runtime,
        env={
            "FLYTE_API_KEY_FILE": str(tmp_path / "unused"),
            "UNION_POLL_INTERVAL_SECONDS": "1",
            "UNION_RETRY_MAX_BACKOFF_SECONDS": "2",
        },
        client_factory=ReconnectingClient,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "last_event_sequence": 0,
        "log_line_count": 0,
    }
    log_path = runtime._prepare_log_file(job_id)

    thread, result, stop_event = executor._start_log_pump(state, log_path)
    progress_path = tmp_path / "jobs" / job_id / "progress" / "progress.json"
    deadline = time.monotonic() + 4
    while not progress_path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    stop_event.set()
    thread.join(timeout=2)

    assert executor.client.calls >= 2
    assert result["reconnects"] >= 1
    assert state["last_event_sequence"] == 2
    assert state["log_line_count"] == 2
    assert json.loads(progress_path.read_text(encoding="utf-8"))["progress_pct"] == 50


def test_union_log_auth_error_does_not_invalidate_control_plane_session(tmp_path: Path) -> None:
    job_id = "job-pod-log-auth"
    runtime = FakeRuntime(tmp_path)

    class PodLogAuthClient:
        def __init__(self, _config: UnionConfig) -> None:
            self.calls = 0
            self.invalidations = 0
            self.auth_starts = 0
            self.state = {"status": "authenticated"}

        def stream_logs(self, _run_name: str):
            self.calls += 1
            raise RuntimeError(
                "the server has asked for the client to provide credentials (pods/log)"
            )
            yield  # pragma: no cover

        def auth_state(self) -> dict:
            return dict(self.state)

        def invalidate_authentication(self, _exc: Exception) -> None:
            self.invalidations += 1
            self.state = {"status": "authentication_required"}

        def start_device_authentication(self, _request_id=None) -> bool:
            self.auth_starts += 1
            return True

    executor = UnionExecutor(
        runtime,
        env={
            "UNION_AUTH_MODE": "device_flow",
            "UNION_POLL_INTERVAL_SECONDS": "1",
            "UNION_RETRY_MAX_BACKOFF_SECONDS": "1",
        },
        client_factory=PodLogAuthClient,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "last_event_sequence": 0,
        "log_line_count": 0,
    }
    log_path = runtime._prepare_log_file(job_id)

    thread, result, stop_event = executor._start_log_pump(state, log_path)
    deadline = time.monotonic() + 2
    while executor.client.calls < 1 and time.monotonic() < deadline:
        time.sleep(0.02)
    stop_event.set()
    thread.join(timeout=2)

    assert result["error"] is not None
    assert executor.client.calls >= 1
    assert executor.client.invalidations == 0
    assert executor.client.auth_starts == 0
    assert executor.client.auth_state()["status"] == "authenticated"


@pytest.mark.parametrize(
    ("phase", "normalized", "terminal"),
    [
        ("QUEUED", "QUEUED", False),
        ("ActionPhase.RUNNING", "RUNNING", False),
        ("SUCCEEDED", "SUCCEEDED", True),
        ("ActionPhase.SUCCEEDED", "SUCCEEDED", True),
        ("ActionPhase.FAILED", "FAILED", True),
        ("ActionPhase.ABORTED", "ABORTED", True),
        ("ActionPhase.TIMED_OUT", "TIMED_OUT", True),
    ],
)
def test_union_run_snapshot_normalizes_sdk_enum_phases(
    phase: str,
    normalized: str,
    terminal: bool,
) -> None:
    snapshot = UnionRunSnapshot(name="run", phase=phase)

    assert snapshot.normalized_phase == normalized
    assert snapshot.terminal is terminal


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
    assert state["result_storage"]["transfer"]["bytes"] == result_archive.stat().st_size
    assert state["result_storage"]["installed"]["categories"]["logs"]["file_count"] == 1
    assert (job_dir / ".worker" / "result-storage.json").is_file()
    assert [status for _, status, _ in runtime.statuses][-1] == "finished"
    running = next(extra for _, status, extra in runtime.statuses if status == "running")
    assert running["details"]["started_at"] == 1000.0
    assert running["details"]["gpu_model"] == "NVIDIA H200"
    assert state["gpu_model"] == "NVIDIA H200"


def test_union_remote_success_does_not_override_algorithm_failure(tmp_path: Path) -> None:
    job_id = "01c427ce-union-runner-failed"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    config_path = job_dir / "config.resolved.yaml"
    config_path.write_text("simulator: {}\n", encoding="utf-8")
    (job_dir / "job_info.json").write_text(json.dumps({"job_id": job_id}), encoding="utf-8")
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)

    class FailedRunnerClient(FakeUnionClient):
        def stream_logs(self, run_name: str):
            yield encode_event("started", 1, {"started_at": 1000.0})
            yield encode_event("artifact", 2, {"artifact": self.artifact})
            yield encode_event("terminal", 3, {"status": "failed", "exit_code": 1})

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=lambda config: FailedRunnerClient(config, result_archive),
    )
    executor.run_job(
        {
            "job_id": job_id,
            "job_name": "union-runner-failed",
            "config_path": f"jobs/{job_id}/config.resolved.yaml",
            "image": "calof/opeva_simulator:sha-test",
            "attempt_number": 1,
            "env": {},
        }
    )

    state = json.loads((job_dir / ".worker" / "union.json").read_text())
    assert state["terminal_status"] == "failed"
    assert state.get("compute_succeeded") is not True
    assert runtime.statuses[-1][1] == "failed"


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
    legacy_run_name = f"opeva-{job_id.replace('-', '')}-a2"
    run_name = deterministic_run_name(job_id, 2)
    state = {
        "schema_version": 1,
        "job_id": job_id,
        "job_name": "recover-submit",
        "attempt": 2,
        "run_name": legacy_run_name,
        "union_run_id": legacy_run_name,
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


def test_union_executor_restart_during_running_recovers_without_duplicate_submission(tmp_path: Path) -> None:
    job_id = "46e427ea-c634-42c9-845d-16a7c1829801"
    job_dir = tmp_path / "jobs" / job_id
    job_dir.mkdir(parents=True)
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    polled = threading.Event()

    class StillRunningClient(FakeUnionClient):
        def get_run(self, run_name: str) -> UnionRunSnapshot:
            polled.set()
            return UnionRunSnapshot(name=run_name, phase="RUNNING", url="https://union/run")

        def stream_logs(self, run_name: str):
            yield encode_event("started", 1, {"started_at": 1000.0})
            yield encode_event(
                "progress",
                2,
                {"progress": {"step_current": 5, "step_total": 10, "progress_pct": 50}},
            )

    first_holder: dict[str, StillRunningClient] = {}

    def first_factory(config: UnionConfig) -> StillRunningClient:
        client = StillRunningClient(config, result_archive)
        first_holder["client"] = client
        return client

    state = {
        "schema_version": 2,
        "job_id": job_id,
        "job_name": "restart-running",
        "attempt": 1,
        "run_name": deterministic_run_name(job_id, 1),
        "input_uri": "s3://bucket/run/input.tar.gz",
        "result_uri": "s3://bucket/run/result.tar.gz",
        "cancel_uri": "s3://bucket/run/cancel.request",
        "terminal": False,
        "submitted": True,
        "last_event_sequence": 0,
        "log_line_count": 0,
        "algorithm_lines_relayed": 0,
    }
    first = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused"), "UNION_POLL_INTERVAL_SECONDS": "1"},
        client_factory=first_factory,
    )
    monitor = threading.Thread(target=first._resume_state, args=(state,))
    monitor.start()
    assert polled.wait(timeout=2)
    first.close()
    monitor.join(timeout=3)

    assert not monitor.is_alive()
    assert state.get("terminal") is False
    assert first_holder["client"].submit_calls == 0

    second_holder: dict[str, FakeUnionClient] = {}

    def second_factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        second_holder["client"] = client
        return client

    recovered_state = json.loads((job_dir / ".worker" / "union.json").read_text(encoding="utf-8"))
    second = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=second_factory,
    )
    second._resume_state(recovered_state)

    assert second_holder["client"].submit_calls == 0
    assert recovered_state["terminal_status"] == "finished"
    assert (job_dir / "results" / "result.json").is_file()


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


def test_union_executor_waits_for_delayed_result_artifact(tmp_path: Path) -> None:
    job_id = "64d8a1cb-c0d2-404a-888f-c1f4ce161f47"
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)

    class DelayedArtifactClient(FakeUnionClient):
        def __init__(self, config: UnionConfig, archive: Path) -> None:
            super().__init__(config, archive)
            self.refresh_calls = 0

        def refresh_artifact(self, result_uri: str, job_id: str) -> dict:
            self.refresh_calls += 1
            if self.refresh_calls < 3:
                raise RuntimeError("result object not found yet")
            return super().refresh_artifact(result_uri, job_id)

    holder: dict[str, DelayedArtifactClient] = {}

    def factory(config: UnionConfig) -> DelayedArtifactClient:
        client = DelayedArtifactClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={
            "FLYTE_API_KEY_FILE": str(tmp_path / "unused"),
            "UNION_POLL_INTERVAL_SECONDS": "1",
            "UNION_ARTIFACT_REFRESH_ATTEMPTS": "4",
        },
        client_factory=factory,
        wait_fn=lambda _seconds: False,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "result_uri": "s3://bucket/run/result.tar.gz",
    }

    artifact = executor._refresh_artifact(state)

    assert artifact["sha256"] == holder["client"].artifact["sha256"]
    assert holder["client"].refresh_calls == 3
    assert any(
        extra.get("details", {}).get("executor_stage") == "union:waiting_for_artifact"
        for _, _, extra in runtime.statuses
    )


def test_union_executor_reauthenticates_while_refreshing_artifact(tmp_path: Path) -> None:
    job_id = "754e4dd5-b3f5-4b65-ae90-cda187b85d68"
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)

    class ReauthArtifactClient(FakeUnionClient):
        def __init__(self, config: UnionConfig, archive: Path) -> None:
            super().__init__(config, archive)
            self.refresh_calls = 0
            self.auth_starts = 0
            self.state = {"status": "authentication_required"}

        def auth_state(self) -> dict:
            return dict(self.state)

        def start_device_authentication(self, _request_id=None) -> bool:
            self.auth_starts += 1
            self.state = {"status": "authenticated"}
            return True

        def refresh_artifact(self, result_uri: str, job_id: str) -> dict:
            self.refresh_calls += 1
            if self.refresh_calls == 1:
                raise RuntimeError("rpc error: unauthenticated: token expired")
            return super().refresh_artifact(result_uri, job_id)

    holder: dict[str, ReauthArtifactClient] = {}

    def factory(config: UnionConfig) -> ReauthArtifactClient:
        client = ReauthArtifactClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={
            "UNION_AUTH_MODE": "device_flow",
            "UNION_POLL_INTERVAL_SECONDS": "1",
            "UNION_ARTIFACT_REFRESH_ATTEMPTS": "3",
        },
        client_factory=factory,
        wait_fn=lambda _seconds: False,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "result_uri": "s3://bucket/run/result.tar.gz",
    }

    artifact = executor._refresh_artifact(state)

    assert artifact["sha256"] == holder["client"].artifact["sha256"]
    assert holder["client"].refresh_calls == 2
    assert holder["client"].auth_starts == 1


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EDQUOT, errno.ENOSPC, errno.EROFS])
def test_union_executor_does_not_retry_permanent_local_io_errors(
    tmp_path: Path,
    error_number: int,
) -> None:
    runtime = FakeRuntime(tmp_path)
    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=lambda _config: object(),
    )

    assert executor._is_retryable_control_plane_error(OSError(error_number, "local failure")) is False


def test_union_executor_aborts_remote_run_if_backend_is_already_stopped(tmp_path: Path) -> None:
    job_id = "54ad46cf-c869-4ef5-a1fc-3299185a4fe1"
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    runtime.backend_status = "stopped"

    class RunningClient(FakeUnionClient):
        def get_run(self, run_name: str) -> UnionRunSnapshot:
            return UnionRunSnapshot(name=run_name, phase="RUNNING", url="https://union/run")

        def stream_logs(self, run_name: str):
            return iter(())

        def refresh_artifact(self, result_uri: str, job_id: str) -> dict:
            raise FileNotFoundError("no artifact after abort")

    holder: dict[str, RunningClient] = {}

    def factory(config: UnionConfig) -> RunningClient:
        client = RunningClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused"), "UNION_POLL_INTERVAL_SECONDS": "1"},
        client_factory=factory,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "result_uri": "s3://bucket/run/result.tar.gz",
        "submitted": True,
        "terminal": False,
        "log_line_count": 0,
        "last_event_sequence": 0,
    }

    executor._monitor(state)

    assert holder["client"].aborted_runs == [state["run_name"]]
    assert state["terminal_status"] == "stopped"
    assert state["requested_terminal_status"] == "stopped"


def test_union_executor_requested_stop_never_downloads_partial_artifact(tmp_path: Path) -> None:
    job_id = "d8308808-2e2c-464c-8445-46632548ae44"
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    runtime.backend_status = "stop_requested"

    class AbortedClient(FakeUnionClient):
        def __init__(self, config: UnionConfig, archive: Path) -> None:
            super().__init__(config, archive)
            self.download_calls = 0

        def get_run(self, run_name: str) -> UnionRunSnapshot:
            return UnionRunSnapshot(name=run_name, phase="ActionPhase.ABORTED", url="https://union/run")

        def stream_logs(self, run_name: str):
            return iter(())

        def download_artifact(self, get_url: str, destination: Path) -> None:
            self.download_calls += 1
            raise AssertionError("stopped Union jobs must not download result artifacts")

    holder: dict[str, AbortedClient] = {}

    def factory(config: UnionConfig) -> AbortedClient:
        client = AbortedClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused"), "UNION_POLL_INTERVAL_SECONDS": "1"},
        client_factory=factory,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "run_url": "https://union/run",
        "result_uri": "s3://bucket/run/result.tar.gz",
        "submitted": True,
        "terminal": False,
        "artifact": dict(holder["client"].artifact),
        "log_line_count": 0,
        "last_event_sequence": 0,
    }

    executor._monitor(state)

    assert state["terminal_status"] == "stopped"
    assert state["requested_terminal_status"] == "stopped"
    assert holder["client"].download_calls == 0
    assert state.get("artifact_installed") is not True
    assert state["artifact_deleted"] is True
    assert holder["client"].deleted is True
    assert runtime.statuses[-1][1] == "stopped"


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


def test_union_executor_keeps_active_run_recoverable_during_device_reauthentication(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path)

    class ReauthClient:
        def __init__(self, _config: UnionConfig) -> None:
            self.calls = 0
            self.auth_starts = 0
            self.state = {"status": "authentication_required"}

        def auth_state(self) -> dict:
            return dict(self.state)

        def start_device_authentication(self, _request_id=None) -> bool:
            self.auth_starts += 1
            self.state = {"status": "authenticated"}
            return True

    executor = UnionExecutor(
        runtime,
        env={
            "UNION_AUTH_MODE": "device_flow",
            "UNION_POLL_INTERVAL_SECONDS": "1",
            "UNION_RETRY_MAX_BACKOFF_SECONDS": "2",
        },
        client_factory=ReauthClient,
        wait_fn=lambda _seconds: False,
    )
    state = {"job_id": "job-reauth", "run_name": "run-reauth", "started_at": 1000.0}

    def operation() -> str:
        executor.client.calls += 1
        if executor.client.calls == 1:
            raise RuntimeError("the server has asked for the client to provide credentials")
        return "recovered"

    assert executor._retry_control_plane(state, "polling_run", operation) == "recovered"
    assert executor.client.auth_starts == 1
    assert "control_plane_unreachable_since" not in state
    assert not any(status == "failed" for _, status, _ in runtime.statuses)


def test_union_executor_retries_eventually_consistent_run_lookup(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path)
    executor = UnionExecutor(
        runtime,
        env={
            "FLYTE_API_KEY_FILE": str(tmp_path / "unused"),
            "UNION_POLL_INTERVAL_SECONDS": "1",
        },
        client_factory=lambda _config: object(),
        wait_fn=lambda _seconds: False,
    )
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("Union Run not found")
        return "visible"

    assert executor._retry_control_plane(
        {"job_id": "job-eventual", "run_name": "run-eventual"},
        "polling_run",
        operation,
    ) == "visible"
    assert calls == 2


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


def test_union_startup_retires_state_for_deleted_orchestrator_job(tmp_path: Path) -> None:
    job_id = "deleted-union-job"
    state_dir = tmp_path / "jobs" / job_id / ".worker"
    state_dir.mkdir(parents=True)
    state_path = state_dir / "union.json"
    request_path = state_dir / "union-recovery-request.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "job_id": job_id,
                "run_name": deterministic_run_name(job_id, 1),
                "terminal": False,
                "orchestrator_ack": False,
            }
        ),
        encoding="utf-8",
    )
    request_path.write_text(
        json.dumps({"job_id": job_id, "request_id": "deleted-request"}),
        encoding="utf-8",
    )
    runtime = FakeRuntime(tmp_path)
    runtime.backend_status = None
    runtime.backend_exists = False
    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=lambda _config: object(),
    )

    executor.on_startup()

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["terminal"] is True
    assert persisted["orchestrator_ack"] is True
    assert persisted["orchestrator_status"] == "deleted"
    assert persisted["terminal_stage"] == "union:orchestrator_job_missing"
    assert persisted["recovery_status"] == "discarded"
    assert executor._recovery_threads == {}
    assert runtime.active == {}
    assert runtime.statuses == []
    assert not request_path.exists()


def test_union_worker_consumes_durable_recovery_request(tmp_path: Path, monkeypatch) -> None:
    job_id = "8c6222fd-durable-recovery-request"
    state_dir = tmp_path / "jobs" / job_id / ".worker"
    state_dir.mkdir(parents=True)
    request_path = state_dir / "union-recovery-request.json"
    request_path.write_text(
        json.dumps({"action": "union_recover_job", "job_id": job_id, "request_id": "request-1"}),
        encoding="utf-8",
    )
    runtime = FakeRuntime(tmp_path)
    runtime.backend_status = "recovering"
    executor = UnionExecutor(
        runtime,
        env={"FLYTE_API_KEY_FILE": str(tmp_path / "unused")},
        client_factory=lambda _config: object(),
    )
    started: list[str] = []
    monkeypatch.setattr(
        executor,
        "_start_requested_recovery",
        lambda requested_job_id: started.append(requested_job_id) or True,
    )

    executor._scan_recovery_requests_if_due(force=True)

    assert started == [job_id]
    assert not request_path.exists()


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


def test_union_successful_compute_retries_local_recovery_without_failed_status(tmp_path: Path) -> None:
    job_id = "f548ad74-b264-47e0-9609-durable-recovery"
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    holder: dict[str, FakeUnionClient] = {}

    def factory(config: UnionConfig) -> FakeUnionClient:
        client = FakeUnionClient(config, result_archive)
        holder["client"] = client
        return client

    executor = UnionExecutor(
        runtime,
        env={
            "FLYTE_API_KEY_FILE": str(tmp_path / "unused"),
            "UNION_RECOVERY_RETRY_INTERVAL_SECONDS": "1",
        },
        client_factory=factory,
        wait_fn=lambda _seconds: False,
    )
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "result_uri": "s3://bucket/run/result.tar.gz",
        "union_phase": "ActionPhase.SUCCEEDED",
        "compute_succeeded": True,
        "compute_finished_at": 1234.0,
    }
    artifact = dict(holder["client"].artifact)
    install_calls = 0

    def install_with_transient_full_disk(current_state, _artifact, _log_path):
        nonlocal install_calls
        install_calls += 1
        if install_calls == 1:
            raise OSError(errno.ENOSPC, "temporary local space pressure")
        current_state["artifact_installed"] = True
        current_state["artifact_deleted"] = True
        executor._save_state(current_state)

    executor._wait_for_recovery_space = lambda *_args: None
    executor._install_artifact = install_with_transient_full_disk

    executor._recover_successful_result(state, artifact, runtime._prepare_log_file(job_id))

    assert install_calls == 2
    assert state["recovery_attempts"] == 1
    assert state["terminal_status"] == "finished"
    assert any(status == "recovering" for _, status, _ in runtime.statuses)
    assert not any(status == "failed" for _, status, _ in runtime.statuses)


def test_union_recovery_slot_serializes_result_installation(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path)
    executor = UnionExecutor(
        runtime,
        env={
            "FLYTE_API_KEY_FILE": str(tmp_path / "unused"),
            "UNION_MAX_CONCURRENT_RECOVERIES": "1",
        },
        client_factory=lambda _config: object(),
    )
    first_entered = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()

    def hold_first_slot() -> None:
        with executor._recovery_slot({"job_id": "recovery-1"}):
            first_entered.set()
            release_first.wait(timeout=2)

    def wait_for_second_slot() -> None:
        with executor._recovery_slot({"job_id": "recovery-2"}):
            second_entered.set()

    first = threading.Thread(target=hold_first_slot)
    second = threading.Thread(target=wait_for_second_slot)
    first.start()
    assert first_entered.wait(timeout=1)
    second.start()
    assert not second_entered.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert second_entered.is_set()
    assert executor._recovery_active == set()
    assert executor._recovery_waiting == set()


def test_union_recovery_space_includes_archive_extract_and_reserve(tmp_path: Path) -> None:
    executor = UnionExecutor(
        FakeRuntime(tmp_path),
        env={
            "FLYTE_API_KEY_FILE": str(tmp_path / "unused"),
            "UNION_RECOVERY_MIN_FREE_GIB": "20",
            "UNION_RECOVERY_UNKNOWN_SIZE_MULTIPLIER": "4",
        },
        client_factory=lambda _config: object(),
    )

    assert executor._required_recovery_space({"size": 100, "unpacked_size": 500}) == (
        100,
        500,
        100 + 500 + 20 * 1024**3,
    )
    assert executor._required_recovery_space({"size": 100}) == (
        100,
        400,
        100 + 400 + 20 * 1024**3,
    )


def test_union_stop_during_recovery_skips_download(tmp_path: Path) -> None:
    job_id = "e3e45d1d-49ed-4dc5-8fdc-stop-recovery"
    result_archive = _make_result_archive(tmp_path, job_id)
    runtime = FakeRuntime(tmp_path)
    runtime.backend_status = "stop_requested"

    class StopRecoveryClient(FakeUnionClient):
        def download_artifact(self, get_url: str, destination: Path) -> None:
            raise AssertionError("stopped recovery must not download the result")

    holder: dict[str, StopRecoveryClient] = {}

    def factory(config: UnionConfig) -> StopRecoveryClient:
        client = StopRecoveryClient(config, result_archive)
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
        "union_phase": "ActionPhase.SUCCEEDED",
        "compute_succeeded": True,
        "artifact": dict(holder["client"].artifact),
    }

    executor._recover_successful_result(
        state,
        dict(holder["client"].artifact),
        runtime._prepare_log_file(job_id),
    )

    assert holder["client"].deleted is True
    assert state.get("artifact_installed") is not True
    assert state["terminal_status"] == "stopped"
    assert runtime.statuses[-1][1] == "stopped"


def test_union_stop_received_during_download_prevents_result_installation(tmp_path: Path) -> None:
    job_id = "ef66323f-stop-after-download"
    result_archive = _make_result_archive(tmp_path, job_id)

    class StopAfterDownloadRuntime(FakeRuntime):
        status_checks = 0

        def _fetch_status(self, current_job_id: str) -> str:
            self.status_checks += 1
            return "running" if self.status_checks == 1 else "stop_requested"

    runtime = StopAfterDownloadRuntime(tmp_path)
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
    state = {
        "job_id": job_id,
        "run_name": deterministic_run_name(job_id, 1),
        "result_uri": "s3://bucket/run/result.tar.gz",
        "compute_succeeded": True,
    }

    with pytest.raises(RuntimeError, match="stopped after download"):
        executor._install_artifact(state, dict(holder["client"].artifact), runtime._prepare_log_file(job_id))

    assert holder["client"].deleted is True
    assert not (tmp_path / "jobs" / job_id / "results" / "result.json").exists()
    assert state["terminal_status"] == "stopped"


def test_union_result_merge_moves_files_without_a_second_copy(tmp_path: Path, monkeypatch) -> None:
    job_id = "job-merge-move"
    extracted = tmp_path / "staging"
    source = extracted / "jobs" / job_id / "results"
    source.mkdir(parents=True)
    (source / "large.bin").write_bytes(b"payload")
    shared = tmp_path / "shared"

    monkeypatch.setattr(shutil, "copy2", lambda *_args, **_kwargs: pytest.fail("unexpected second copy"))

    merge_job_results(extracted, shared, job_id)

    assert (shared / "jobs" / job_id / "results" / "large.bin").read_bytes() == b"payload"
    assert not (source / "large.bin").exists()
