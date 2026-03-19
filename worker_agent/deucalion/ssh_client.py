from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path


class SSHCommandError(RuntimeError):
    def __init__(self, message: str, returncode: int | None = None, stdout: str = "", stderr: str = "") -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@dataclass
class SSHSettings:
    host: str
    user: str
    port: int
    key_path: str
    known_hosts_path: str
    connect_timeout: int = 10


class SSHClient:
    def __init__(self, settings: SSHSettings) -> None:
        self.settings = settings

    def _ssh_base(self) -> list[str]:
        return [
            "-p",
            str(self.settings.port),
            "-i",
            self.settings.key_path,
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={self.settings.connect_timeout}",
            "-o",
            f"UserKnownHostsFile={self.settings.known_hosts_path}",
            "-o",
            "StrictHostKeyChecking=yes",
        ]

    def _remote(self) -> str:
        return f"{self.settings.user}@{self.settings.host}"

    def run(self, command: str, timeout: int = 60, check: bool = True) -> str:
        cmd = ["ssh", *self._ssh_base(), self._remote(), command]
        completed = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        # SSH transport/auth failures are reported with rc=255 and should always
        # be treated as connectivity errors, even when command check=False.
        if completed.returncode == 255:
            raise SSHCommandError(
                f"SSH connectivity/auth failed while running: {command}",
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        if check and completed.returncode != 0:
            raise SSHCommandError(
                f"SSH command failed ({completed.returncode}): {command}",
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        return completed.stdout.strip()

    def copy_to(
        self,
        local_path: str | Path,
        remote_path: str,
        timeout: int = 60,
        recursive: bool = False,
    ) -> None:
        cmd = ["scp"]
        if recursive:
            cmd.append("-r")
        cmd.extend(
            [
                *self._ssh_base(),
                str(local_path),
                f"{self._remote()}:{remote_path}",
            ]
        )
        completed = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
        if completed.returncode != 0:
            raise SSHCommandError(
                f"SCP to remote failed ({completed.returncode})",
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )

    def copy_from(self, remote_path: str, local_path: str | Path, timeout: int = 60, recursive: bool = False) -> None:
        cmd = ["scp"]
        if recursive:
            cmd.append("-r")
        cmd.extend(
            [
                *self._ssh_base(),
                f"{self._remote()}:{remote_path}",
                str(local_path),
            ]
        )
        completed = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
        if completed.returncode != 0:
            raise SSHCommandError(
                f"SCP from remote failed ({completed.returncode})",
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
