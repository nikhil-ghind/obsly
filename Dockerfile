FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

COPY obsly ./obsly

RUN pip install --no-cache-dir -e .

# Default command is overridden per-service in docker-compose.yml
CMD ["python", "-m", "obsly.cli.exporter"]
