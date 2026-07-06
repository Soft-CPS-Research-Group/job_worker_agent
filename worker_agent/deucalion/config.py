from __future__ import annotations

from dataclasses import dataclass, field
import posixpath
from pathlib import PurePosixPath
from typing import Any, Mapping


DEFAULT_DEUCALION_REMOTE_ROOT = "/projects/F202508843CPCAA0/tiagocalof"

DEUCALION_PARTITION_WALLTIME_LIMIT_SECONDS = {
    "dev-arm": 4 * 60 * 60,
    "normal-arm": 48 * 60 * 60,
    "large-arm": 72 * 60 * 60,
    "dev-x86": 4 * 60 * 60,
    "normal-x86": 48 * 60 * 60,
    "large-x86": 72 * 60 * 60,
    "dev-a100-40": 4 * 60 * 60,
    "normal-a100-40": 48 * 60 * 60,
    "dev-a100-80": 4 * 60 * 60,
    "normal-a100-80": 48 * 60 * 60,
}


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
    sif_path_explicit: bool
    sif_image: str | None
    sif_version: str | None
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


def _first_non_none(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) is not None:
            return mapping.get(key)
    return None


def _parse_command_mode(value: Any, default: str = "run") -> str:
    mode = _as_str(value, default).lower()
    if mode not in {"run", "exec"}:
        raise ValueError(f"Invalid deucalion command_mode: {mode!r}. Expected 'run' or 'exec'")
    return mode


def _is_gpu_partition(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip().lower()
    return bool(normalized) and ("gpu" in normalized or "a100" in normalized or "h100" in normalized)


def _format_walltime_limit(seconds: int) -> str:
    hours = seconds // 3600
    if seconds % 3600 == 0:
        return f"{hours} hour" if hours == 1 else f"{hours} hours"
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _parse_slurm_time_limit_seconds(value: Any) -> int:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError("Slurm time limit must not be empty")

    days = 0
    time_part = text
    has_day_prefix = "-" in text
    if has_day_prefix:
        day_part, time_part = text.split("-", 1)
        if not day_part.isdigit() or not time_part:
            raise ValueError(f"Invalid Slurm time limit: {text!r}")
        days = int(day_part)

    parts = time_part.split(":")
    if not 1 <= len(parts) <= 3 or any(not part.isdigit() for part in parts):
        raise ValueError(f"Invalid Slurm time limit: {text!r}")
    values = [int(part) for part in parts]

    if has_day_prefix:
        hours = values[0]
        minutes = values[1] if len(values) >= 2 else 0
        seconds = values[2] if len(values) >= 3 else 0
    elif len(values) == 1:
        hours = 0
        minutes = values[0]
        seconds = 0
    elif len(values) == 2:
        hours = 0
        minutes, seconds = values
    else:
        hours, minutes, seconds = values

    if minutes >= 60 or seconds >= 60:
        raise ValueError(f"Invalid Slurm time limit: {text!r}")

    total = days * 24 * 3600 + hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        raise ValueError("Slurm time limit must be greater than zero")
    return total


def _validate_deucalion_walltime(profile: SlurmProfile) -> None:
    partition = profile.partition.strip().lower()
    max_seconds = DEUCALION_PARTITION_WALLTIME_LIMIT_SECONDS.get(partition)
    if max_seconds is None:
        allowed = ", ".join(DEUCALION_PARTITION_WALLTIME_LIMIT_SECONDS)
        raise ValueError(f"Unknown Deucalion partition {profile.partition!r}. Allowed: {allowed}")

    requested_seconds = _parse_slurm_time_limit_seconds(profile.time_limit)
    if requested_seconds > max_seconds:
        max_label = _format_walltime_limit(max_seconds)
        raise ValueError(
            f"Deucalion partition {partition!r} has a {max_label} walltime limit; "
            f"requested {profile.time_limit!r}"
        )
    profile.partition = partition


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
    if pure.name == "schema.json" and len(pure.parts) >= 3:
        normalized = str(PurePosixPath(*pure.parts[:-1]))
    return normalized


def _as_relative_dataset_list(value: Any) -> list[str]:
    return [_validate_relative_dataset_path(p) for p in _as_list(value)]


def _infer_dataset_root_from_path(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("\\", "/")
    if normalized.startswith("/data/"):
        normalized = normalized[len("/data/") :]
    normalized = normalized.lstrip("/")
    if not normalized.startswith("datasets/"):
        return None
    pure = PurePosixPath(normalized)
    parts = pure.parts
    if len(parts) < 2:
        return None
    dataset_root = str(PurePosixPath("datasets", parts[1]))
    return _validate_relative_dataset_path(dataset_root)


def _infer_datasets_from_config(config: dict[str, Any] | None) -> list[str]:
    if not isinstance(config, dict):
        return []
    simulator = config.get("simulator")
    if not isinstance(simulator, dict):
        return []

    inferred: list[str] = []
    seen: set[str] = set()

    def _append_dataset(path: str | None) -> None:
        if not path or path in seen:
            return
        seen.add(path)
        inferred.append(path)

    for key in ("dataset_path", "dataset_paths"):
        raw = simulator.get(key)
        for entry in _as_list(raw):
            _append_dataset(_infer_dataset_root_from_path(entry))

    dataset_name = simulator.get("dataset_name")
    if dataset_name is not None:
        maybe_path = _infer_dataset_root_from_path(f"datasets/{str(dataset_name).strip()}")
        _append_dataset(maybe_path)

    return inferred


def resolve_deucalion_job_config(
    config: dict[str, Any] | None,
    env: Mapping[str, str] | None = None,
    runtime_options: Mapping[str, Any] | None = None,
) -> DeucalionJobConfig:
    env = env or {}
    execution = (config or {}).get("execution", {}) if isinstance(config, dict) else {}
    if not isinstance(execution, dict):
        execution = {}
    deucalion = execution.get("deucalion", {})
    if not isinstance(deucalion, dict):
        deucalion = {}
    options = dict(runtime_options or {})
    source = options if options else deucalion

    env_gpus = _parse_int("DEUCALION_SLURM_GPUS", env.get("DEUCALION_SLURM_GPUS"), 0)
    raw_gpus = _pick(source, "gpus")
    raw_partition = _pick(source, "partition")
    gpus = _parse_int("deucalion_options.gpus", raw_gpus, env_gpus)
    if raw_gpus is None and gpus == 0 and _is_gpu_partition(raw_partition):
        gpus = 1

    account_default = env.get("DEUCALION_SLURM_ACCOUNT_GPU") if gpus > 0 else env.get("DEUCALION_SLURM_ACCOUNT_CPU")
    if not account_default:
        account_default = "f202508843cpcaa0g" if gpus > 0 else "f202508843cpcaa0x"

    partition_default = env.get("DEUCALION_SLURM_PARTITION_GPU") if gpus > 0 else env.get("DEUCALION_SLURM_PARTITION_CPU")
    if not partition_default:
        partition_default = "normal-a100-80" if gpus > 0 else "normal-x86"

    profile = SlurmProfile(
        account=_as_str(_pick(source, "account"), account_default),
        partition=_as_str(raw_partition, partition_default),
        time_limit=_as_str(_first_non_none(source, "time_limit", "time"), env.get("DEUCALION_SLURM_TIME", "04:00:00")),
        cpus_per_task=_parse_int(
            "deucalion_options.cpus_per_task",
            _pick(source, "cpus_per_task"),
            _parse_int("DEUCALION_SLURM_CPUS_PER_TASK", env.get("DEUCALION_SLURM_CPUS_PER_TASK"), 4),
        ),
        mem_gb=_parse_int(
            "deucalion_options.mem_gb",
            _pick(source, "mem_gb"),
            _parse_int("DEUCALION_SLURM_MEM_GB", env.get("DEUCALION_SLURM_MEM_GB"), 8),
        ),
        gpus=max(0, gpus),
        modules=_as_list(_pick(source, "modules")) or _as_list(env.get("DEUCALION_MODULES")),
    )
    if _is_gpu_partition(profile.partition) and profile.gpus <= 0:
        raise ValueError(f"Deucalion GPU partition {profile.partition!r} requires gpus > 0")
    if profile.gpus > 0 and not _is_gpu_partition(profile.partition):
        raise ValueError(
            f"Deucalion gpus={profile.gpus} requires a GPU partition, got {profile.partition!r}"
        )
    _validate_deucalion_walltime(profile)

    remote_root = _as_str(
        env.get("DEUCALION_REMOTE_ROOT"),
        DEFAULT_DEUCALION_REMOTE_ROOT,
    ).rstrip("/")
    sif_path_raw = _pick(source, "sif_path")
    env_sif_path = env.get("DEUCALION_SIF_PATH")
    sif_path_explicit = bool(sif_path_raw is not None or (env_sif_path and env_sif_path.strip()))
    sif_path = _as_str(
        sif_path_raw,
        env_sif_path or posixpath.join(remote_root, "images", "cache", "simulator.sif"),
    )

    sif_image = _as_str(
        _pick(source, "sif_image"),
        env.get("DEUCALION_SIF_IMAGE", ""),
    )
    if not sif_image:
        sif_image = None

    sif_version = _as_str(
        _pick(source, "sif_version"),
        env.get("DEUCALION_SIF_VERSION", ""),
    )
    if not sif_version:
        sif_version = None

    command_mode = _parse_command_mode(
        _pick(source, "command_mode"),
        env.get("DEUCALION_SIF_COMMAND_MODE", "run"),
    )
    source_datasets = _pick(source, "datasets")
    if source_datasets is None:
        datasets = _infer_datasets_from_config(config if isinstance(config, dict) else None)
    else:
        datasets = _as_relative_dataset_list(source_datasets)
    required_paths = _as_list(_pick(source, "required_paths"))

    return DeucalionJobConfig(
        remote_root=remote_root,
        sif_path=sif_path,
        sif_path_explicit=sif_path_explicit,
        sif_image=sif_image,
        sif_version=sif_version,
        command_mode=command_mode,
        datasets=datasets,
        required_paths=required_paths,
        profile=profile,
    )
