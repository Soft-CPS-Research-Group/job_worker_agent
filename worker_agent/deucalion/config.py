from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


DEFAULT_DEUCALION_REMOTE_ROOT = "/projects/F202508843CPCAA0/tiagocalof"


@dataclass
class SlurmProfile:
    account: str
    partition: str
    time_limit: str
    cpus_per_task: int
    mem_gb: int
    gpus: int
    modules: list[str] = field(default_factory=list)


@dataclass
class DeucalionJobConfig:
    remote_root: str
    sif_path: str
    required_paths: list[str]
    profile: SlurmProfile


def _as_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_str(value: Any, default: str) -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return [str(value).strip()]


def _pick(mapping: dict[str, Any], key: str) -> Any:
    value = mapping.get(key)
    return value if value is not None else None


def resolve_deucalion_job_config(config: dict[str, Any] | None, env: Mapping[str, str] | None = None) -> DeucalionJobConfig:
    env = env or {}
    execution = (config or {}).get("execution", {}) if isinstance(config, dict) else {}
    if not isinstance(execution, dict):
        execution = {}
    deucalion = execution.get("deucalion", {})
    if not isinstance(deucalion, dict):
        deucalion = {}

    gpus = _as_int(_pick(deucalion, "gpus"), _as_int(env.get("DEUCALION_SLURM_GPUS"), 0))

    account_default = env.get("DEUCALION_SLURM_ACCOUNT_GPU") if gpus > 0 else env.get("DEUCALION_SLURM_ACCOUNT_CPU")
    if not account_default:
        account_default = "f202508843cpcaa0g" if gpus > 0 else "f202508843cpcaa0x"

    partition_default = env.get("DEUCALION_SLURM_PARTITION_GPU") if gpus > 0 else env.get("DEUCALION_SLURM_PARTITION_CPU")
    if not partition_default:
        partition_default = "normal-a100-80" if gpus > 0 else "normal-x86"

    profile = SlurmProfile(
        account=_as_str(_pick(deucalion, "account"), account_default),
        partition=_as_str(_pick(deucalion, "partition"), partition_default),
        time_limit=_as_str(_pick(deucalion, "time"), env.get("DEUCALION_SLURM_TIME", "04:00:00")),
        cpus_per_task=_as_int(_pick(deucalion, "cpus_per_task"), _as_int(env.get("DEUCALION_SLURM_CPUS_PER_TASK"), 4)),
        mem_gb=_as_int(_pick(deucalion, "mem_gb"), _as_int(env.get("DEUCALION_SLURM_MEM_GB"), 8)),
        gpus=max(0, gpus),
        modules=_as_list(_pick(deucalion, "modules")) or _as_list(env.get("DEUCALION_MODULES")),
    )

    sif_path = _as_str(_pick(deucalion, "sif_path"), env.get("DEUCALION_SIF_PATH", ""))
    if not sif_path:
        raise ValueError("Missing Deucalion SIF path. Set DEUCALION_SIF_PATH or execution.deucalion.sif_path")

    remote_root = _as_str(
        env.get("DEUCALION_REMOTE_ROOT"),
        DEFAULT_DEUCALION_REMOTE_ROOT,
    ).rstrip("/")

    required_paths = _as_list(_pick(deucalion, "required_paths"))

    return DeucalionJobConfig(
        remote_root=remote_root,
        sif_path=sif_path,
        required_paths=required_paths,
        profile=profile,
    )
