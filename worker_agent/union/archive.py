from __future__ import annotations

import os
import shutil
import tarfile
from pathlib import Path, PurePosixPath
from typing import Iterable

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
        temporary = destination.with_name(f".{destination.name}.union.tmp")
        shutil.copy2(source_path, temporary)
        os.replace(temporary, destination)


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
