from __future__ import annotations

import errno
import json
import os
import shutil
import tarfile
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

import yaml

from worker_agent.deucalion.config import infer_datasets_from_config


def _safe_relative_path(value: str) -> Path:
    normalized = str(PurePosixPath(value.replace("\\", "/")))
    path = Path(normalized)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Path must be relative to the shared data root: {value!r}")
    return path


def _within(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def load_resolved_config(shared_dir: Path, config_path: str) -> tuple[Path, dict]:
    relative = _safe_relative_path(config_path)
    path = shared_dir / relative
    if not path.is_file() or not _within(shared_dir, path):
        raise FileNotFoundError(f"Resolved config not found under shared data: {config_path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Resolved config must contain a YAML mapping: {config_path}")
    return path, payload


def collect_input_paths(shared_dir: Path, job_id: str, config_path: str) -> list[Path]:
    config_file, config = load_resolved_config(shared_dir, config_path)
    job_dir = shared_dir / "jobs" / job_id
    candidates: list[Path] = [config_file]
    job_info = job_dir / "job_info.json"
    if job_info.is_file():
        candidates.append(job_info)

    for dataset in infer_datasets_from_config(config):
        dataset_path = shared_dir / _safe_relative_path(dataset)
        if not dataset_path.exists() or not _within(shared_dir, dataset_path):
            raise FileNotFoundError(f"Dataset referenced by config is missing: {dataset}")
        candidates.append(dataset_path)

    result: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        result.append(candidate)
    return result


def create_input_archive(shared_dir: Path, job_id: str, config_path: str, output_path: Path) -> list[str]:
    paths = collect_input_paths(shared_dir, job_id, config_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    archived: list[str] = []
    with tarfile.open(output_path, "w:gz") as archive:
        archive.dereference = True
        for path in paths:
            relative = path.resolve().relative_to(shared_dir.resolve())
            archive.add(path, arcname=relative.as_posix(), recursive=True)
            archived.append(relative.as_posix())
    return archived


def safe_extract(archive_path: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            member_path = Path(member.name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(f"Unsafe path in archive: {member.name}")
            if member.issym() or member.islnk() or member.isdev():
                raise ValueError(f"Unsupported archive member: {member.name}")
            target = (destination / member_path).resolve()
            if target != root and root not in target.parents:
                raise ValueError(f"Archive member escapes destination: {member.name}")
        archive.extractall(destination)


def merge_job_results(extracted_root: Path, shared_dir: Path, job_id: str) -> None:
    source = extracted_root / "jobs" / job_id
    if not source.is_dir():
        raise FileNotFoundError(f"Result archive does not contain jobs/{job_id}")
    target = shared_dir / "jobs" / job_id
    target.mkdir(parents=True, exist_ok=True)

    for source_path in source.rglob("*"):
        relative = source_path.relative_to(source)
        if relative.as_posix() in {
            "job_info.json",
            ".worker/union.json",
            f"logs/{job_id}.log",
        }:
            continue
        destination = target / relative
        if source_path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue
        if source_path.is_symlink():
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Staging lives below the same job directory, so replace avoids a
            # second full copy of large exports during final installation.
            os.replace(source_path, destination)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            temporary = destination.with_name(f".{destination.name}.union.tmp")
            shutil.copy2(source_path, temporary)
            os.replace(temporary, destination)


_STORAGE_CATEGORIES = ("kpis", "timeseries", "checkpoints", "logs", "other")


def _storage_category(relative: Path) -> str:
    parts = tuple(part.lower() for part in relative.parts)
    name = relative.name.lower()
    suffix = relative.suffix.lower()

    if "checkpoints" in parts or "checkpoint" in name:
        return "checkpoints"
    if "kpi" in name and suffix in {".csv", ".json", ".parquet", ".xlsx"}:
        return "kpis"
    if (
        "timeseries" in parts
        or "time_series" in parts
        or name.startswith("exported_")
        or name.startswith("timeseries_")
    ) and suffix in {".csv", ".json", ".parquet", ".feather", ".arrow"}:
        return "timeseries"
    if "logs" in parts or suffix in {".log", ".out", ".err"}:
        return "logs"
    return "other"


def measure_job_storage(job_dir: Path) -> dict[str, Any]:
    categories = {
        category: {"bytes": 0, "file_count": 0}
        for category in _STORAGE_CATEGORIES
    }
    total_bytes = 0
    file_count = 0

    if job_dir.is_dir():
        for path in job_dir.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(job_dir)
            if relative.parts and relative.parts[0] == ".worker":
                continue
            try:
                size = path.stat().st_size
            except OSError:
                continue
            category = _storage_category(relative)
            categories[category]["bytes"] += size
            categories[category]["file_count"] += 1
            total_bytes += size
            file_count += 1

    return {
        "bytes": total_bytes,
        "file_count": file_count,
        "categories": categories,
    }


def write_result_storage_manifest(
    job_dir: Path,
    *,
    transferred_bytes: int | None,
    announced_unpacked_bytes: int,
    announced_file_count: int,
) -> dict[str, Any]:
    installed = measure_job_storage(job_dir)
    payload = {
        "schema_version": 1,
        "measured_at": time.time(),
        "transfer": {
            "bytes": max(0, int(transferred_bytes)) if transferred_bytes is not None else None,
            "announced_unpacked_bytes": max(0, int(announced_unpacked_bytes)),
            "announced_file_count": max(0, int(announced_file_count)),
        },
        "installed": installed,
    }

    worker_dir = job_dir / ".worker"
    worker_dir.mkdir(parents=True, exist_ok=True)
    target = worker_dir / "result-storage.json"
    fd, temporary_path = tempfile.mkstemp(
        dir=str(worker_dir),
        prefix=".result-storage.",
        suffix=".json.tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, 0o666)
        os.replace(temporary_path, target)
    finally:
        try:
            if os.path.exists(temporary_path):
                os.remove(temporary_path)
        except OSError:
            pass
    return payload


def append_missing_algorithm_logs(local_log: Path, extracted_root: Path, job_id: str, relayed_lines: int) -> int:
    remote_log = extracted_root / "jobs" / job_id / "logs" / f"{job_id}.log"
    if not remote_log.is_file():
        return relayed_lines
    total_lines = 0
    skip_lines = max(0, relayed_lines)
    with remote_log.open("r", encoding="utf-8", errors="replace") as source, local_log.open(
        "a",
        encoding="utf-8",
    ) as destination:
        for total_lines, line in enumerate(source, start=1):
            if total_lines > skip_lines:
                destination.write(line)
    return total_lines


def paths_as_strings(paths: Iterable[Path]) -> list[str]:
    return [str(path) for path in paths]
