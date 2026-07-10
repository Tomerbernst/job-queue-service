FROM python:3.11-slim

WORKDIR /srv/app

# curl is needed for the compose healthcheck
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY app ./app
COPY tests ./tests
RUN pip install --no-cache-dir ".[dev]"

RUN useradd --create-home appuser
USER appuser

# default: API. The worker service overrides the command in docker-compose.
CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
