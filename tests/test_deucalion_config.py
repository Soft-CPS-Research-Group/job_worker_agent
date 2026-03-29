import pytest

from worker_agent.deucalion.config import resolve_deucalion_job_config


def test_resolve_config_cpu_defaults():
    cfg = resolve_deucalion_job_config(
        config={"execution": {"deucalion": {"sif_path": "/remote/sim.sif"}}},
        env={},
    )
    assert cfg.profile.gpus == 0
    assert cfg.profile.account == "f202508843cpcaa0x"
    assert cfg.profile.partition == "normal-x86"
    assert cfg.command_mode == "run"
    assert cfg.datasets == []
    assert cfg.sif_version is None


def test_resolve_config_gpu_and_yaml_override():
    cfg = resolve_deucalion_job_config(
        config={
            "execution": {
                "deucalion": {
                    "sif_path": "/remote/sim.sif",
                    "gpus": 1,
                    "partition": "dev-a100-80",
                    "account": "custom-gpu-account",
                    "time": "01:00:00",
                    "command_mode": "exec",
                    "datasets": ["datasets/site_a/input.csv"],
                }
            }
        },
        env={
            "DEUCALION_SLURM_ACCOUNT_GPU": "f202508843cpcaa0g",
            "DEUCALION_SLURM_PARTITION_GPU": "normal-a100-80",
        },
    )
    assert cfg.profile.gpus == 1
    assert cfg.profile.partition == "dev-a100-80"
    assert cfg.profile.account == "custom-gpu-account"
    assert cfg.profile.time_limit == "01:00:00"
    assert cfg.command_mode == "exec"
    assert cfg.datasets == ["datasets/site_a/input.csv"]


def test_resolve_config_command_mode_env_default():
    cfg = resolve_deucalion_job_config(
        config={"execution": {"deucalion": {"sif_path": "/remote/sim.sif"}}},
        env={"DEUCALION_SIF_COMMAND_MODE": "exec"},
    )
    assert cfg.command_mode == "exec"


def test_resolve_config_sif_version_from_yaml_and_env():
    cfg = resolve_deucalion_job_config(
        config={"execution": {"deucalion": {"sif_path": "/remote/sim.sif", "sif_version": "v0.2.5"}}},
        env={"DEUCALION_SIF_VERSION": "v0.2.4"},
    )
    assert cfg.sif_version == "v0.2.5"

    cfg_env = resolve_deucalion_job_config(
        config={"execution": {"deucalion": {"sif_path": "/remote/sim.sif"}}},
        env={"DEUCALION_SIF_VERSION": "v0.2.4"},
    )
    assert cfg_env.sif_version == "v0.2.4"


@pytest.mark.parametrize(
    "field,value",
    [
        ("gpus", "x"),
        ("cpus_per_task", "two"),
        ("mem_gb", "16g"),
    ],
)
def test_resolve_config_invalid_int_values_fail_fast(field, value):
    with pytest.raises(ValueError):
        resolve_deucalion_job_config(
            config={"execution": {"deucalion": {"sif_path": "/remote/sim.sif", field: value}}},
            env={},
        )


def test_resolve_config_rejects_absolute_dataset_path():
    with pytest.raises(ValueError):
        resolve_deucalion_job_config(
            config={
                "execution": {
                    "deucalion": {
                        "sif_path": "/remote/sim.sif",
                        "datasets": ["/opt/opeva_shared_data/datasets/a.csv"],
                    }
                }
            },
            env={},
        )


def test_resolve_config_rejects_parent_dataset_path():
    with pytest.raises(ValueError):
        resolve_deucalion_job_config(
            config={
                "execution": {
                    "deucalion": {
                        "sif_path": "/remote/sim.sif",
                        "datasets": ["datasets/../secret.txt"],
                    }
                }
            },
            env={},
        )
