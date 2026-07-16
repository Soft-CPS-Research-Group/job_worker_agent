from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, TypeVar
from urllib.parse import urlparse

from .archive import safe_extract
from .events import encode_event


_T = TypeVar("_T")


class EventEmitter:
    def __init__(self) -> None:
        self.sequence = 0

    def emit(self, kind: str, **payload: Any) -> None:
        self.sequence += 1
        print(encode_event(kind, self.sequence, {"timestamp": time.time(), **payload}), flush=True)


class S3Store:
    def __init__(self) -> None:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:  # pragma: no cover - production image contract
            raise RuntimeError("boto3 is required by the Union runner image") from exc

        ca_path = _install_ca_bundle()
        endpoint = os.environ.get("FLYTE_AWS_ENDPOINT") or None
        access_key = os.environ.get("FLYTE_AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("FLYTE_AWS_SECRET_ACCESS_KEY")
        session_token = os.environ.get("FLYTE_AWS_SESSION_TOKEN")
        region = os.environ.get("FLYTE_AWS_REGION", "us-east-1")
        if not access_key or not secret_key:
            raise RuntimeError("Union did not inject object-store credentials into the runner pod")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region,
            verify=ca_path or True,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 10, "mode": "standard"},
                connect_timeout=15,
                read_timeout=120,
                tcp_keepalive=True,
            ),
        )

    @staticmethod
    def split_uri(uri: str) -> tuple[str, str]:
        parsed = urlparse(uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
            raise ValueError(f"Expected an s3:// URI, got {uri!r}")
        return parsed.netloc, parsed.path.lstrip("/")

    def download(self, uri: str, destination: Path) -> None:
        bucket, key = self.split_uri(uri)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(bucket, key, str(destination))

    def upload(self, source: Path, uri: str, sha256: str) -> None:
        bucket, key = self.split_uri(uri)
        self.client.upload_file(
            str(source),
            bucket,
            key,
            ExtraArgs={"Metadata": {"opeva-sha256": sha256}},
        )

    def delete(self, uri: str) -> None:
        bucket, key = self.split_uri(uri)
        self.client.delete_object(Bucket=bucket, Key=key)

    def exists(self, uri: str) -> bool:
        bucket, key = self.split_uri(uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            response = getattr(exc, "response", {})
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode") if isinstance(response, dict) else None
            if status == 404:
                return False
            raise
        return True

    def artifact(self, uri: str, expires: int) -> dict[str, Any]:
        bucket, key = self.split_uri(uri)
        head = self.client.head_object(Bucket=bucket, Key=key)
        metadata = head.get("Metadata") if isinstance(head, dict) else {}
        return {
            "uri": uri,
            "size": int(head.get("ContentLength", 0)),
            "sha256": str((metadata or {}).get("opeva-sha256") or ""),
            "get_url": self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expires,
            ),
            "delete_url": self.client.generate_presigned_url(
                "delete_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expires,
            ),
        }

    def presign_put(self, uri: str, expires: int) -> str:
        bucket, key = self.split_uri(uri)
        return self.client.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )


def _install_ca_bundle() -> str | None:
    encoded = os.environ.get("OPEVA_OBJECT_STORE_CA_B64", "").strip()
    if not encoded:
        return None
    try:
        payload = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise RuntimeError("OPEVA_OBJECT_STORE_CA_B64 is not valid base64") from exc
    path = Path(tempfile.gettempdir()) / "opeva-union-object-store-ca.pem"
    path.write_bytes(payload)
    os.chmod(path, 0o600)
    os.environ["AWS_CA_BUNDLE"] = str(path)
    os.environ["REQUESTS_CA_BUNDLE"] = str(path)
    return str(path)


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing runner environment variable: {name}")
    return value


def _positive_int(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    return max(1, value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _retry_store_operation(
    description: str,
    operation: Callable[[], _T],
    *,
    attempts: int = 8,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> _T:
    delay = 1
    last_error: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            print(
                f"[union-runner] {description} failed; retrying ({attempt}/{attempts}): {exc}",
                file=sys.stderr,
                flush=True,
            )
            sleep_fn(delay)
            delay = min(delay * 2, 30)
    assert last_error is not None
    raise last_error


def _cancel_object_exists(store: S3Store, cancel_uri: str) -> bool:
    try:
        return store.exists(cancel_uri)
    except Exception as exc:
        # Cancellation has a control-plane abort fallback. A transient object
        # store probe must never terminate an otherwise healthy long run.
        print(
            f"[union-runner] Unable to check cancellation marker; training continues: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return False


def _archive_job(data_root: Path, job_id: str, output: Path) -> None:
    job_dir = data_root / "jobs" / job_id
    if not job_dir.is_dir():
        raise FileNotFoundError(f"Job directory was not created: {job_dir}")
    with tarfile.open(output, "w:gz") as archive:
        archive.dereference = True
        archive.add(job_dir, arcname=f"jobs/{job_id}", recursive=True)


def _relay_log(log_path: Path, offset: int) -> int:
    if not log_path.is_file():
        return offset
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(offset)
        for line in handle:
            print(f"[algorithms] {line.rstrip()}", flush=True)
        return handle.tell()


def _read_progress(path: Path) -> tuple[str | None, dict[str, Any] | None]:
    if not path.is_file():
        return None, None
    try:
        raw = path.read_text(encoding="utf-8")
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None, None
    if not isinstance(value, dict):
        return None, None
    fingerprint = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return fingerprint, value


def _read_gpu_model(path: Path) -> str | None:
    try:
        value = " ".join(path.read_text(encoding="utf-8").split())
    except OSError:
        return None
    return value[:160] or None


def _wait_for(path: Path, timeout: int, description: str) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {description} after {timeout}s")
        time.sleep(1)


def run_job() -> int:
    emitter = EventEmitter()
    store = S3Store()
    job_id = _required("OPEVA_JOB_ID")
    input_uri = _required("OPEVA_INPUT_URI")
    result_uri = _required("OPEVA_RESULT_URI")
    cancel_uri = _required("OPEVA_CANCEL_URI")
    data_root = Path(os.environ.get("OPEVA_DATA_ROOT", "/data"))
    setup_timeout = _positive_int("OPEVA_SETUP_TIMEOUT_SECONDS", 3600)
    run_timeout = _positive_int("OPEVA_RUN_TIMEOUT_SECONDS", 2592000)
    artifact_ttl = min(604800, _positive_int("OPEVA_ARTIFACT_URL_TTL_SECONDS", 3600))
    marker_dir = data_root / ".opeva"
    marker_dir.mkdir(parents=True, exist_ok=True)
    sidecar_ready = marker_dir / "sidecar.ready"
    input_ready = marker_dir / "input.ready"
    started_marker = marker_dir / "algorithm.started"
    gpu_model_path = marker_dir / "gpu.model"
    done_marker = marker_dir / "algorithm.done"
    cancel_marker = marker_dir / "cancel"
    exit_path = marker_dir / "exit.code"
    log_path = data_root / "jobs" / job_id / "logs" / f"{job_id}.log"
    progress_path = data_root / "jobs" / job_id / "progress" / "progress.json"
    input_archive = Path(tempfile.gettempdir()) / f"opeva-{job_id}-input.tar.gz"
    result_archive = Path(tempfile.gettempdir()) / f"opeva-{job_id}-result.tar.gz"
    canceled = False
    exit_code = 1

    emitter.emit(
        "setup",
        phase="waiting_for_algorithms_image",
        cancel_put_url=store.presign_put(cancel_uri, min(604800, run_timeout)),
    )
    try:
        _wait_for(sidecar_ready, setup_timeout, "Algorithms sidecar readiness")
        emitter.emit("setup", phase="downloading_input")
        _retry_store_operation("input download", lambda: store.download(input_uri, input_archive))
        safe_extract(input_archive, data_root)
        try:
            _retry_store_operation("input cleanup", lambda: store.delete(input_uri), attempts=3)
        except Exception as exc:
            print(f"[union-runner] Input cleanup deferred: {exc}", file=sys.stderr, flush=True)
        input_archive.unlink(missing_ok=True)

        input_ready.touch()
        emitter.emit("setup", phase="waiting_for_algorithms_start")
        _wait_for(started_marker, setup_timeout, "Algorithms process start")
        started_at = started_marker.stat().st_mtime
        gpu_model = _read_gpu_model(gpu_model_path)
        emitter.emit("started", started_at=started_at, **({"gpu_model": gpu_model} if gpu_model else {}))

        deadline = time.monotonic() + run_timeout
        next_cancel_probe = 0.0
        log_offset = 0
        progress_fingerprint: str | None = None
        while not done_marker.exists():
            log_offset = _relay_log(log_path, log_offset)
            fingerprint, progress = _read_progress(progress_path)
            if fingerprint and fingerprint != progress_fingerprint and progress is not None:
                progress_fingerprint = fingerprint
                emitter.emit("progress", progress=progress)
            now = time.monotonic()
            if not canceled and now >= next_cancel_probe and _cancel_object_exists(store, cancel_uri):
                canceled = True
                cancel_marker.touch()
                try:
                    store.delete(cancel_uri)
                except Exception as exc:
                    print(
                        f"[union-runner] Cancellation marker cleanup deferred: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                emitter.emit("setup", phase="cancel_requested")
            next_cancel_probe = now + 5
            if now >= deadline:
                canceled = True
                cancel_marker.touch()
                emitter.emit("setup", phase="run_timeout")
                _wait_for(done_marker, 90, "Algorithms shutdown after timeout")
                break
            time.sleep(2)

        log_offset = _relay_log(log_path, log_offset)
        fingerprint, progress = _read_progress(progress_path)
        if fingerprint and fingerprint != progress_fingerprint and progress is not None:
            emitter.emit("progress", progress=progress)
        try:
            exit_code = int(exit_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            exit_code = 1
    except Exception as exc:
        print(f"[union-runner] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        exit_code = 1
    finally:
        try:
            if (data_root / "jobs" / job_id).is_dir():
                _archive_job(data_root, job_id, result_archive)
                digest = _sha256(result_archive)
                _retry_store_operation(
                    "result upload",
                    lambda: store.upload(result_archive, result_uri, digest),
                )
                artifact = _retry_store_operation(
                    "result metadata lookup",
                    lambda: store.artifact(result_uri, artifact_ttl),
                )
                emitter.emit("artifact", artifact=artifact)
        except Exception as exc:
            print(f"[union-runner] Failed to publish result artifact: {exc}", file=sys.stderr, flush=True)
            if exit_code == 0:
                exit_code = 1
        result_archive.unlink(missing_ok=True)
        input_archive.unlink(missing_ok=True)
        for temporary_uri in (input_uri, cancel_uri):
            try:
                store.delete(temporary_uri)
            except Exception as exc:
                print(
                    f"[union-runner] Failed to clean temporary object {temporary_uri}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

    terminal_status = "stopped" if canceled else ("finished" if exit_code == 0 else "failed")
    emitter.emit("terminal", status=terminal_status, exit_code=exit_code)
    return 0 if exit_code == 0 else max(1, min(255, abs(exit_code)))


def sign_artifact() -> int:
    emitter = EventEmitter()
    store = S3Store()
    result_uri = _required("OPEVA_RESULT_URI")
    artifact_ttl = min(604800, _positive_int("OPEVA_ARTIFACT_URL_TTL_SECONDS", 3600))
    try:
        artifact = _retry_store_operation(
            "result metadata lookup",
            lambda: store.artifact(result_uri, artifact_ttl),
        )
        emitter.emit("artifact", artifact=artifact)
        emitter.emit("terminal", status="finished", exit_code=0)
        return 0
    except Exception as exc:
        print(f"[union-runner] Unable to sign artifact: {exc}", file=sys.stderr, flush=True)
        emitter.emit("terminal", status="failed", exit_code=1)
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OPEVA helper inside a Union task pod")
    parser.add_argument("mode", choices=("run", "sign"))
    args = parser.parse_args()
    raise SystemExit(run_job() if args.mode == "run" else sign_artifact())


if __name__ == "__main__":  # pragma: no cover
    main()
