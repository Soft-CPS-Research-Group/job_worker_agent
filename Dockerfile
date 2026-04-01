FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY worker_agent ./worker_agent

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client curl ca-certificates tar \
    && ORAS_VERSION=1.2.3 \
    && curl -sSL "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_amd64.tar.gz" -o /tmp/oras.tgz \
    && tar -xzf /tmp/oras.tgz -C /tmp oras \
    && install -m 0755 /tmp/oras /usr/local/bin/oras \
    && rm -f /tmp/oras /tmp/oras.tgz \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir .

ENTRYPOINT ["job-worker-agent"]
