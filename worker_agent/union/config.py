from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


def _positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = str(env.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def _required(env: Mapping[str, str], name: str, default: str = "") -> str:
    value = str(env.get(name, default)).strip()
    if not value:
        raise ValueError(f"Missing required Union setting: {name}")
    return value


@dataclass(frozen=True)
class UnionConfig:
    auth_mode: str
    endpoint: str
    org: str
    project: str
    domain: str
    api_key_file: Path | None
    runner_image: str
    object_store_ca_file: Path | None
    control_plane_ca_file: Path | None
    runner_cpu: str
    runner_memory: str
    job_cpu: str
    job_memory: str
    gpu_count: int
    setup_timeout_seconds: int
    run_timeout_seconds: int
    graceful_stop_timeout_seconds: int
    artifact_url_ttl_seconds: int
    artifact_refresh_attempts: int
    poll_interval_seconds: int
    status_update_interval_seconds: int
    unreachable_grace_seconds: int
    retry_max_backoff_seconds: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "UnionConfig":
        ca_raw = str(env.get("UNION_OBJECT_STORE_CA_FILE", "")).strip()
        control_plane_ca_raw = str(env.get("UNION_CONTROL_PLANE_CA_FILE", "")).strip()
        auth_mode = str(env.get("UNION_AUTH_MODE", "api_key")).strip().lower().replace("-", "_")
        if auth_mode not in {"api_key", "device_flow"}:
            raise ValueError("UNION_AUTH_MODE must be 'api_key' or 'device_flow'")
        api_key_raw = str(env.get("FLYTE_API_KEY_FILE", "/run/secrets/union_api_key")).strip()
        return cls(
            auth_mode=auth_mode,
            endpoint=_required(env, "UNION_ENDPOINT", "dns:///inesctec.hosted.unionai.cloud"),
            org=_required(env, "UNION_ORG", "inesctec"),
            project=_required(env, "UNION_PROJECT", "humanise-energaize"),
            domain=_required(env, "UNION_DOMAIN", "development"),
            api_key_file=Path(api_key_raw) if auth_mode == "api_key" else None,
            runner_image=_required(env, "UNION_RUNNER_IMAGE", "calof/job_worker_agent:union-latest"),
            object_store_ca_file=Path(ca_raw) if ca_raw else None,
            control_plane_ca_file=Path(control_plane_ca_raw) if control_plane_ca_raw else None,
            runner_cpu=_required(env, "UNION_RUNNER_CPU", "1"),
            runner_memory=_required(env, "UNION_RUNNER_MEMORY", "2Gi"),
            job_cpu=_required(env, "UNION_JOB_CPU", "4"),
            job_memory=_required(env, "UNION_JOB_MEMORY", "16Gi"),
            gpu_count=_positive_int(env, "UNION_GPU_COUNT", 1),
            setup_timeout_seconds=_positive_int(env, "UNION_SETUP_TIMEOUT_SECONDS", 3600),
            run_timeout_seconds=_positive_int(env, "UNION_RUN_TIMEOUT_SECONDS", 2592000),
            graceful_stop_timeout_seconds=_positive_int(env, "UNION_GRACEFUL_STOP_TIMEOUT_SECONDS", 120),
            artifact_url_ttl_seconds=min(
                604800,
                _positive_int(env, "UNION_ARTIFACT_URL_TTL_SECONDS", 3600),
            ),
            artifact_refresh_attempts=_positive_int(env, "UNION_ARTIFACT_REFRESH_ATTEMPTS", 6),
            poll_interval_seconds=_positive_int(env, "UNION_POLL_INTERVAL_SECONDS", 10),
            status_update_interval_seconds=_positive_int(env, "UNION_STATUS_UPDATE_INTERVAL_SECONDS", 30),
            unreachable_grace_seconds=_positive_int(env, "UNION_UNREACHABLE_GRACE_SECONDS", 900),
            retry_max_backoff_seconds=_positive_int(env, "UNION_RETRY_MAX_BACKOFF_SECONDS", 60),
        )

    def read_api_key(self) -> str:
        if self.api_key_file is None:
            raise RuntimeError("Union API key is unavailable in device-flow mode")
        try:
            value = self.api_key_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError(f"Unable to read Union API key from {self.api_key_file}: {exc}") from exc
        if not value:
            raise RuntimeError(f"Union API key file is empty: {self.api_key_file}")
        return value

    def read_ca_bundle(self) -> str | None:
        if self.object_store_ca_file is None:
            return None
        try:
            value = self.object_store_ca_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError(
                f"Unable to read Union object-store CA from {self.object_store_ca_file}: {exc}"
            ) from exc
        if not value:
            raise RuntimeError(f"Union object-store CA file is empty: {self.object_store_ca_file}")
        return value + "\n"
