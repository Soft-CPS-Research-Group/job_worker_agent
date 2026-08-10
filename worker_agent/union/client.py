from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import logging
from pathlib import Path
import shlex
import time
import threading
import weakref
from typing import Any, Iterator, Mapping

import requests

from .config import UnionConfig
from .events import parse_event


_LOGGER = logging.getLogger(__name__)
_DEVICE_AUTH_HOOK_LOCK = threading.Lock()
_DEVICE_AUTH_OBSERVERS: weakref.WeakSet["FlyteUnionClient"] = weakref.WeakSet()
_DEVICE_AUTH_HOOKS_INSTALLED = False


def _install_device_auth_hooks(client: "FlyteUnionClient") -> None:
    """Observe every SDK Device Flow, including refresh failures during active runs."""
    global _DEVICE_AUTH_HOOKS_INSTALLED
    from flyte.remote._client.auth._authenticators import device_code

    with _DEVICE_AUTH_HOOK_LOCK:
        _DEVICE_AUTH_OBSERVERS.add(client)
        if _DEVICE_AUTH_HOOKS_INSTALLED:
            return
        original_get_device_code = device_code.token_client.get_device_code
        original_poll_token_endpoint = device_code.token_client.poll_token_endpoint

        async def observed_get_device_code(*args: Any, **kwargs: Any):
            response = await original_get_device_code(*args, **kwargs)
            for observer in list(_DEVICE_AUTH_OBSERVERS):
                observer._device_authorization_required(response)
            return response

        async def observed_poll_token_endpoint(*args: Any, **kwargs: Any):
            try:
                result = await original_poll_token_endpoint(*args, **kwargs)
            except Exception as exc:
                for observer in list(_DEVICE_AUTH_OBSERVERS):
                    observer._device_authorization_failed(exc)
                raise
            for observer in list(_DEVICE_AUTH_OBSERVERS):
                observer._device_authorization_completed()
            return result

        device_code.token_client.get_device_code = observed_get_device_code
        device_code.token_client.poll_token_endpoint = observed_poll_token_endpoint
        _DEVICE_AUTH_HOOKS_INSTALLED = True


@dataclass(frozen=True)
class UnionRunSnapshot:
    name: str
    phase: str
    url: str | None = None

    @property
    def normalized_phase(self) -> str:
        """Return the SDK phase name without its enum/class prefix."""
        return self.phase.rsplit(".", 1)[-1].upper()

    @property
    def terminal(self) -> bool:
        return self.normalized_phase in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED_OUT"}


def derive_object_uris(input_uri: str) -> tuple[str, str]:
    if "/" not in input_uri.rstrip("/"):
        raise ValueError(f"Cannot derive result location from input URI: {input_uri!r}")
    parent = input_uri.rsplit("/", 1)[0]
    return f"{parent}/result.tar.gz", f"{parent}/cancel.request"


def deterministic_run_name(job_id: str, attempt: int) -> str:
    identity = f"{job_id.strip().lower()}:{max(1, int(attempt))}"
    return f"opeva-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24]}"


def artifact_signer_run_name(job_id: str, nonce: int | None = None) -> str:
    identity = f"{job_id.strip().lower()}:{nonce if nonce is not None else time.time_ns()}"
    return f"opeva-sign-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:19]}"


def _algorithms_wrapper(job_id: str, command: str, setup_timeout: int) -> str:
    tokens = shlex.split(command)
    if tokens and tokens[0] == "python":
        algorithm_command = tokens
    else:
        algorithm_command = ["python", "run_experiment.py", *tokens]
    command_text = " ".join(shlex.quote(token) for token in algorithm_command)
    marker_root = "/data/.opeva"
    gpu_model_path = marker_root + "/gpu.model"
    log_path = f"/data/jobs/{job_id}/logs/{job_id}.log"
    return f"""
set +e
mkdir -p {shlex.quote(marker_root)} {shlex.quote(str(Path(log_path).parent))}
code=1
child=""
finish() {{
  if [ -n "$child" ] && kill -0 "$child" 2>/dev/null; then
    kill -TERM "$child" 2>/dev/null || true
  fi
  printf '%s\n' "$code" > {shlex.quote(marker_root + '/exit.code')}
  touch {shlex.quote(marker_root + '/algorithm.done')}
}}
trap finish EXIT
touch {shlex.quote(marker_root + '/sidecar.ready')}
deadline=$(( $(date +%s) + {int(setup_timeout)} ))
while [ ! -f {shlex.quote(marker_root + '/input.ready')} ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    code=124
    exit 0
  fi
  sleep 1
done
gpu_model=""
if command -v nvidia-smi >/dev/null 2>&1; then
  gpu_model="$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -n 1 | tr -d '\r')"
fi
if [ -z "$gpu_model" ]; then
  gpu_model="$(python -c 'import torch; print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else "")' 2>/dev/null)"
fi
if [ -n "$gpu_model" ]; then
  printf '%s\n' "$gpu_model" > {shlex.quote(gpu_model_path + '.tmp')}
  mv {shlex.quote(gpu_model_path + '.tmp')} {shlex.quote(gpu_model_path)}
fi
{command_text} >> {shlex.quote(log_path)} 2>&1 &
child=$!
touch {shlex.quote(marker_root + '/algorithm.started')}
while kill -0 "$child" 2>/dev/null; do
  if [ -f {shlex.quote(marker_root + '/cancel')} ]; then
    kill -TERM "$child" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "$child" 2>/dev/null || break
      sleep 1
    done
    kill -KILL "$child" 2>/dev/null || true
  fi
  sleep 2
done
wait "$child"
code=$?
child=""
exit 0
""".strip()


class FlyteUnionClient:
    def __init__(self, config: UnionConfig) -> None:
        self.config = config
        self._initialized = False
        self._auth_lock = threading.RLock()
        self._auth_thread: threading.Thread | None = None
        self._auth_state: dict[str, Any] = {
            "status": "authenticated" if config.auth_mode == "api_key" else "checking",
            "updated_at": time.time(),
        }
        if config.auth_mode == "device_flow":
            _install_device_auth_hooks(self)

    def auth_state(self) -> dict[str, Any]:
        with self._auth_lock:
            return dict(self._auth_state)

    def _set_auth_state(self, status: str, **extra: Any) -> None:
        with self._auth_lock:
            previous = dict(self._auth_state)
            preserved = {
                key: previous[key]
                for key in ("last_verified_at", "verified_surfaces")
                if key in previous
            }
            self._auth_state = {"status": status, "updated_at": time.time(), **preserved, **extra}

    def mark_verified(self, surface: str) -> None:
        if self.config.auth_mode != "device_flow":
            return
        now = time.time()
        with self._auth_lock:
            surfaces = dict(self._auth_state.get("verified_surfaces") or {})
            surfaces[str(surface)] = now
            self._auth_state = {
                "status": "authenticated",
                "updated_at": now,
                "last_verified_at": now,
                "verified_surfaces": surfaces,
            }

    def invalidate_authentication(self, exc: Exception) -> None:
        if self.config.auth_mode != "device_flow":
            return
        self._initialized = False
        previous = self.auth_state()
        self._set_auth_state(
            "authentication_required",
            error=str(exc),
            **{
                key: previous[key]
                for key in ("verification_url", "verification_url_complete", "user_code", "expires_at")
                if key in previous
            },
        )

    @staticmethod
    def _is_authentication_error(exc: Exception) -> bool:
        response = getattr(exc, "response", None)
        if getattr(response, "status_code", None) == 401:
            return True
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "unauthenticated",
                "authentication failed",
                "token expired",
                "provide credentials",
                "invalid api key",
            )
        )

    def _device_authorization_required(self, response: Any) -> None:
        previous = self.auth_state()
        verification_url = str(response.verification_uri)
        separator = "&" if "?" in verification_url else "?"
        self._set_auth_state(
            "authentication_required",
            verification_url=verification_url,
            verification_url_complete=f"{verification_url}{separator}user_code={response.user_code}",
            user_code=str(response.user_code),
            expires_at=time.time() + int(response.expires_in),
            **({"request_id": previous["request_id"]} if previous.get("request_id") else {}),
        )

    def _device_authorization_completed(self) -> None:
        self._set_auth_state("authenticated")

    def _device_authorization_failed(self, exc: Exception) -> None:
        previous = self.auth_state()
        self._set_auth_state(
            "authentication_required",
            error=str(exc),
            **{
                key: previous[key]
                for key in (
                    "verification_url",
                    "verification_url_complete",
                    "user_code",
                    "expires_at",
                    "request_id",
                )
                if key in previous
            },
        )

    def start_device_authentication(self, request_id: str | None = None) -> bool:
        if self.config.auth_mode != "device_flow":
            return False
        with self._auth_lock:
            if self._auth_thread and self._auth_thread.is_alive():
                return False
            self._set_auth_state("checking", request_id=request_id)
            self._auth_thread = threading.Thread(
                target=self._run_device_authentication,
                name="union-device-auth",
                daemon=True,
            )
            self._auth_thread.start()
        return True

    def ensure_authentication_fresh(self, max_age_seconds: int) -> None:
        if self.config.auth_mode != "device_flow":
            return
        state = self.auth_state()
        if state.get("status") != "authenticated":
            return
        last_verified = float(state.get("last_verified_at") or state.get("updated_at") or 0)
        if time.time() - last_verified < max(1, int(max_age_seconds)):
            return
        with self._auth_lock:
            if self._auth_thread and self._auth_thread.is_alive():
                return
            self._set_auth_state("checking", request_id="periodic-verification")
            self._auth_thread = threading.Thread(
                target=self._run_authentication_verification,
                name="union-auth-verify",
                daemon=True,
            )
            self._auth_thread.start()

    def _run_authentication_verification(self) -> None:
        try:
            from flyte.remote import Run

            next(iter(Run.listall(limit=1)), None)
            self._initialized = True
            self.mark_verified("api")
        except Exception as exc:
            if self._is_authentication_error(exc):
                _LOGGER.warning("Union credentials are invalid; reopening Device Flow: %s", exc)
                self.invalidate_authentication(exc)
                self._run_device_authentication()
            else:
                _LOGGER.warning("Union credential verification was inconclusive: %s", exc)
                self._set_auth_state("authenticated", verification_error=str(exc))

    def _run_device_authentication(self) -> None:
        try:
            import flyte
            from flyte.remote import Run

            flyte.init(
                endpoint=self.config.endpoint,
                org=self.config.org,
                project=self.config.project,
                domain=self.config.domain,
                headless=True,
                auth_type="DeviceFlow",
                ca_cert_file_path=(
                    str(self.config.control_plane_ca_file) if self.config.control_plane_ca_file else None
                ),
                image_builder="remote",
            )
            # Force an authenticated control-plane call; flyte.init itself is lazy.
            next(iter(Run.listall(limit=1)), None)
            self._initialized = True
            self.mark_verified("api")
        except Exception as exc:
            _LOGGER.warning("Union device authentication did not complete: %s", exc)
            self._initialized = False
            previous = self.auth_state()
            self._set_auth_state(
                "authentication_required",
                error=str(exc),
                **({"request_id": previous["request_id"]} if previous.get("request_id") else {}),
            )

    def initialize(self) -> None:
        if self._initialized:
            return
        try:
            import flyte
        except ImportError as exc:  # pragma: no cover - image contract
            raise RuntimeError("Union executor requires the worker 'union' dependency extra") from exc
        if self.config.auth_mode == "device_flow":
            if not self._initialized:
                self.start_device_authentication()
                raise RuntimeError("Union authentication is required before jobs can be submitted")
            return
        ca_path = str(self.config.control_plane_ca_file) if self.config.control_plane_ca_file else None
        flyte.init(
            endpoint=self.config.endpoint,
            api_key=self.config.read_api_key(),
            org=self.config.org,
            project=self.config.project,
            domain=self.config.domain,
            headless=True,
            ca_cert_file_path=ca_path,
            image_builder="remote",
        )
        self._initialized = True

    def upload_input(self, path: Path, job_id: str, attempt: int) -> str:
        self.initialize()
        from flyte.remote import upload_file

        _, uri = upload_file(path, fname=f"opeva-{job_id}-a{attempt}-input.tar.gz")
        return str(uri)

    def _ca_b64(self) -> str:
        bundle = self.config.read_ca_bundle()
        if not bundle:
            return ""
        return base64.b64encode(bundle.encode("utf-8")).decode("ascii")

    def _pod_task(
        self,
        *,
        job: Mapping[str, Any],
        input_uri: str,
        result_uri: str,
        cancel_uri: str,
    ):
        import flyte
        from flyte.extras import ContainerTask
        from kubernetes.client import (
            V1Container,
            V1EmptyDirVolumeSource,
            V1EnvVar,
            V1PodSpec,
            V1ResourceRequirements,
            V1Volume,
            V1VolumeMount,
        )

        job_id = str(job["job_id"])
        job_image = str(job.get("image") or "").strip()
        if not job_image:
            raise ValueError("Union job payload is missing the Algorithms image")
        command = str(job.get("command") or "").strip()
        if not command:
            command = f"--config /data/{str(job['config_path']).lstrip('/')} --job_id {job_id}"
        ca_b64 = self._ca_b64()
        runner_env = {
            "OPEVA_JOB_ID": job_id,
            "OPEVA_INPUT_URI": input_uri,
            "OPEVA_RESULT_URI": result_uri,
            "OPEVA_CANCEL_URI": cancel_uri,
            "OPEVA_SETUP_TIMEOUT_SECONDS": str(self.config.setup_timeout_seconds),
            "OPEVA_RUN_TIMEOUT_SECONDS": str(self.config.run_timeout_seconds),
            "OPEVA_ARTIFACT_URL_TTL_SECONDS": str(self.config.artifact_url_ttl_seconds),
            "OPEVA_OBJECT_STORE_CA_B64": ca_b64,
            "PYTHONUNBUFFERED": "1",
        }
        algorithm_env = {
            **{str(key): str(value) for key, value in dict(job.get("env") or {}).items()},
            "PYTHONUNBUFFERED": "1",
        }
        volume_mount = V1VolumeMount(name="opeva-data", mount_path="/data")
        gpu_resource = str(self.config.gpu_count)
        pod = V1PodSpec(
            restart_policy="Never",
            containers=[
                V1Container(
                    name="primary",
                    image=self.config.runner_image,
                    image_pull_policy="Always",
                    env=[V1EnvVar(name=key, value=value) for key, value in runner_env.items()],
                    volume_mounts=[volume_mount],
                ),
                V1Container(
                    name="algorithms",
                    image=job_image,
                    image_pull_policy="Always",
                    command=["bash", "-lc"],
                    args=[_algorithms_wrapper(job_id, command, self.config.setup_timeout_seconds)],
                    env=[V1EnvVar(name=key, value=value) for key, value in algorithm_env.items()],
                    volume_mounts=[volume_mount],
                    resources=V1ResourceRequirements(
                        requests={
                            "cpu": self.config.job_cpu,
                            "memory": self.config.job_memory,
                            "nvidia.com/gpu": gpu_resource,
                        },
                        limits={
                            "cpu": self.config.job_cpu,
                            "memory": self.config.job_memory,
                            "nvidia.com/gpu": gpu_resource,
                        },
                    ),
                ),
            ],
            volumes=[V1Volume(name="opeva-data", empty_dir=V1EmptyDirVolumeSource())],
        )
        pod_template = flyte.PodTemplate.from_spec(
            pod,
            primary_container_name="primary",
            labels={"opeva-job-id": job_id, "opeva-worker": "union-inesctec"},
        )
        task = ContainerTask(
            name=f"opeva-union-{job_id.replace('-', '')[:20]}",
            image=self.config.runner_image,
            command=["python", "-m", "worker_agent.union.runner", "run"],
            resources=flyte.Resources(cpu=self.config.runner_cpu, memory=self.config.runner_memory),
            timeout=timedelta(seconds=self.config.setup_timeout_seconds + self.config.run_timeout_seconds + 300),
            pod_template=pod_template,
        )
        flyte.TaskEnvironment.from_task(f"opeva-union-{job_id.replace('-', '')[:20]}", task)
        return task

    def submit_job(
        self,
        *,
        job: Mapping[str, Any],
        run_name: str,
        input_uri: str,
        result_uri: str,
        cancel_uri: str,
    ) -> UnionRunSnapshot:
        self.initialize()
        import flyte

        task = self._pod_task(
            job=job,
            input_uri=input_uri,
            result_uri=result_uri,
            cancel_uri=cancel_uri,
        )
        try:
            run = flyte.with_runcontext(
                name=run_name,
                version=run_name,
                copy_style="none",
                labels={"opeva-job-id": str(job["job_id"]), "opeva-worker": "union-inesctec"},
            ).run(task)
        except Exception as exc:
            # Submission may have reached Union even if the response was lost.
            # The deterministic run name lets recovery adopt that execution for
            # any submission error, without relying on SDK-specific wording.
            try:
                return self.get_run(run_name)
            except Exception:
                raise exc
        return UnionRunSnapshot(name=run.name, phase=str(run.phase), url=str(run.url))

    def get_run(self, run_name: str) -> UnionRunSnapshot:
        self.initialize()
        from flyte.remote import Run

        run = Run.get(name=run_name)
        return UnionRunSnapshot(name=run.name, phase=str(run.phase), url=str(run.url))

    def stream_logs(self, run_name: str) -> Iterator[str]:
        self.initialize()
        from flyte.remote import Run

        run = Run.get(name=run_name)
        yield from run.get_logs(filter_system=True, show_ts=False)

    def abort(self, run_name: str, reason: str) -> None:
        self.initialize()
        from flyte.remote import Run

        Run.get(name=run_name).abort(reason=reason)

    def request_graceful_cancel(self, put_url: str) -> None:
        response = requests.put(put_url, data=b"cancel\n", timeout=30)
        response.raise_for_status()

    def download_artifact(self, get_url: str, destination: Path) -> None:
        verify: str | bool = str(self.config.object_store_ca_file) if self.config.object_store_ca_file else True
        with requests.get(get_url, stream=True, timeout=(30, 300), verify=verify) as response:
            response.raise_for_status()
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)

    def delete_artifact(self, delete_url: str) -> None:
        verify: str | bool = str(self.config.object_store_ca_file) if self.config.object_store_ca_file else True
        response = requests.delete(delete_url, timeout=30, verify=verify)
        if response.status_code == 404:
            return
        response.raise_for_status()

    def refresh_artifact(self, result_uri: str, job_id: str) -> dict[str, Any]:
        self.initialize()
        import flyte
        from flyte.extras import ContainerTask
        from flyte.remote import Run
        from kubernetes.client import V1Container, V1EnvVar, V1PodSpec

        env = {
            "OPEVA_RESULT_URI": result_uri,
            "OPEVA_ARTIFACT_URL_TTL_SECONDS": str(self.config.artifact_url_ttl_seconds),
            "OPEVA_SIGNER_LOG_GRACE_SECONDS": str(self.config.signer_log_grace_seconds),
            "OPEVA_OBJECT_STORE_CA_B64": self._ca_b64(),
        }
        pod = V1PodSpec(
            restart_policy="Never",
            containers=[
                V1Container(
                    name="primary",
                    image=self.config.runner_image,
                    image_pull_policy="Always",
                    env=[V1EnvVar(name=key, value=value) for key, value in env.items()],
                )
            ],
        )
        task = ContainerTask(
            name=f"opeva-sign-{job_id.replace('-', '')[:20]}",
            image=self.config.runner_image,
            command=["python", "-m", "worker_agent.union.runner", "sign"],
            resources=flyte.Resources(cpu="1", memory="1Gi"),
            timeout=timedelta(minutes=10),
            pod_template=flyte.PodTemplate.from_spec(pod, primary_container_name="primary"),
        )
        flyte.TaskEnvironment.from_task(f"opeva-sign-{job_id.replace('-', '')[:20]}", task)
        run_name = artifact_signer_run_name(job_id)
        run = flyte.with_runcontext(name=run_name, version=run_name, copy_style="none").run(task)
        last_error: Exception | None = None
        terminal_polls = 0
        for _ in range(300):
            try:
                current = Run.get(name=run_name)
                for line in current.get_logs(filter_system=True, show_ts=False):
                    event = parse_event(line)
                    if event and event.get("kind") == "artifact" and isinstance(event.get("artifact"), dict):
                        return dict(event["artifact"])
            except Exception as exc:  # pragma: no cover - remote timing dependent
                last_error = exc
            try:
                phase = str(Run.get(name=run_name).phase).rsplit(".", 1)[-1].upper()
            except Exception as exc:  # pragma: no cover - remote timing dependent
                last_error = exc
                phase = ""
            if phase in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED_OUT"}:
                terminal_polls += 1
                if terminal_polls >= 3:
                    break
            time.sleep(2)
        raise RuntimeError(f"Union signer task did not return artifact URLs: {last_error or run_name}")
