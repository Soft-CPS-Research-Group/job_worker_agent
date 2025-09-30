FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY worker_agent ./worker_agent

RUN pip install --no-cache-dir .

ENTRYPOINT ["job-worker-agent"]
