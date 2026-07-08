# Laptop Worker Notes

Notas rapidas para usar o `tiago-laptop` como worker local com GPU.

## Configurar uma vez

Cria `/home/tiago/dev/job_worker_agent/.local-worker.env` a partir do exemplo:

```bash
cd /home/tiago/dev/job_worker_agent
cp .local-worker.env.example .local-worker.env
```

Valores esperados:

```bash
WORKER_ID=tiago-laptop
OPEVA_SERVER=http://193.136.62.78:8011
NFS_SERVER=193.136.62.78
NFS_EXPORT=/opt/opeva_shared_data
MOUNT_POINT=/mnt/opeva_shared
NFS_MOUNT_OPTS=vers=4.1,proto=tcp,port=2049
VPN_CONNECTION=deinet
VPN_REQUIRED=1
VPN_WATCHDOG=1
VPN_TARGET=193.136.62.78
WORKER_AGENT_IMAGE=job_worker_agent:local
WORKER_JOB_IMAGE=calof/opeva_simulator:latest
WORKER_EXECUTOR=docker
WORKER_ENABLE_GPU=true
WORKER_REQUIRE_GPU=true
WORKER_REMAP_DATA_VOLUME=true
SHUTDOWN_TIMEOUT=900
PULL_BEFORE_START=0
```

Antes de servir jobs, confirma que Docker ve a GPU:

```bash
docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi
```

## Comandos do dia-a-dia

Por o laptop a servir:

```bash
cd /home/tiago/dev/job_worker_agent
sudo scripts/local_worker.sh serve
```

O `serve` tenta subir a VPN `VPN_CONNECTION` via NetworkManager se ela nao
estiver ativa, monta o NFS e arranca um watchdog leve. O watchdog verifica
periodicamente VPN/NFS e tenta recuperar a ligacao sem depender da sessao
grafica.

Ver estado do worker e mount:

```bash
sudo scripts/local_worker.sh status
```

Ver logs do worker:

```bash
sudo scripts/local_worker.sh logs
```

Parar de forma controlada:

```bash
sudo scripts/local_worker.sh stop
```

O `stop` normal:

- deixa de aceitar jobs novos;
- se estiver idle, sai logo;
- se estiver a correr um job, deixa acabar;
- publica o estado final no orchestrator;
- desmonta o NFS no fim.

Abortar imediatamente:

```bash
sudo scripts/local_worker.sh stop --force
```

Usa `stop --force` so quando quiseres matar o job atual. Esse caminho marca o job como `failed` com `error="force-stop"`.

Se quiseres montar/desmontar sem arrancar o worker:

```bash
sudo scripts/local_worker.sh mount
sudo scripts/local_worker.sh umount
```

Subir apenas a VPN:

```bash
sudo scripts/local_worker.sh vpn
```

## Notas importantes

- O orchestrator deployed tem de ter `tiago-laptop` em `AVAILABLE_HOSTS`.
- O laptop tem de conseguir chegar ao orchestrator pela VPN em `OPEVA_SERVER`.
- O NFS tem de montar corretamente; sem NFS o worker ate pode aparecer online, mas os jobs nao vao ver configs/results/logs corretamente.
- A VPN tem de estar guardada no NetworkManager como ligacao de sistema/headless.
  Se os secrets estiverem presos ao user keyring da sessao grafica, o `serve`
  vai falhar ao executar `nmcli connection up`.
- `WORKER_REQUIRE_GPU=true` evita fallback silencioso para CPU.
