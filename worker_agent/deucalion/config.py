from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
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
    command_mode: str
    datasets: list[str]
    required_paths: list[str]
    profile: SlurmProfile


def _parse_int(name: str, value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer value for {name}: {value!r}") from exc


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


def _parse_command_mode(value: Any, default: str = "run") -> str:
    mode = _as_str(value, default).lower()
    if mode not in {"run", "exec"}:
        raise ValueError(f"Invalid execution.deucalion.command_mode: {mode!r}. Expected 'run' or 'exec'")
    return mode


def _validate_relative_dataset_path(path: str) -> str:
    raw = path.strip()
    if not raw:
        raise ValueError("Dataset paths must be non-empty")
    pure = PurePosixPath(raw)
    if pure.is_absolute():
        raise ValueError(f"Dataset path must be relative, got absolute path: {raw!r}")
    if ".." in pure.parts:
        raise ValueError(f"Dataset path must not contain '..': {raw!r}")
    normalized = str(pure)
    if not normalized.startswith("datasets/"):
        raise ValueError(f"Dataset path must start with 'datasets/': {raw!r}")
    return normalized


def _as_relative_dataset_list(value: Any) -> list[str]:
    return [_validate_relative_dataset_path(p) for p in _as_list(value)]


def resolve_deucalion_job_config(config: dict[str, Any] | None, env: Mapping[str, str] | None = None) -> DeucalionJobConfig:
    env = env or {}
    execution = (config or {}).get("execution", {}) if isinstance(config, dict) else {}
    if not isinstance(execution, dict):
        execution = {}
    deucalion = execution.get("deucalion", {})
    if not isinstance(deucalion, dict):
        deucalion = {}

    env_gpus = _parse_int("DEUCALION_SLURM_GPUS", env.get("DEUCALION_SLURM_GPUS"), 0)
    gpus = _parse_int("execution.deucalion.gpus", _pick(deucalion, "gpus"), env_gpus)

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
        cpus_per_task=_parse_int(
            "execution.deucalion.cpus_per_task",
            _pick(deucalion, "cpus_per_task"),
            _parse_int("DEUCALION_SLURM_CPUS_PER_TASK", env.get("DEUCALION_SLURM_CPUS_PER_TASK"), 4),
        ),
        mem_gb=_parse_int(
            "execution.deucalion.mem_gb",
            _pick(deucalion, "mem_gb"),
            _parse_int("DEUCALION_SLURM_MEM_GB", env.get("DEUCALION_SLURM_MEM_GB"), 8),
        ),
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

    command_mode = _parse_command_mode(
        _pick(deucalion, "command_mode"),
        env.get("DEUCALION_SIF_COMMAND_MODE", "run"),
    )
    datasets = _as_relative_dataset_list(_pick(deucalion, "datasets"))
    required_paths = _as_list(_pick(deucalion, "required_paths"))

    return DeucalionJobConfig(
        remote_root=remote_root,
        sif_path=sif_path,
        command_mode=command_mode,
        datasets=datasets,
        required_paths=required_paths,
        profile=profile,
    )
