#!/usr/bin/env bash
set -e

if [[ -n "${DATABASE_URL:-}" ]] && [[ "${DATABASE_URL}" == postgres://* ]]; then
  export DATABASE_URL="${DATABASE_URL/postgres:\/\//postgresql+asyncpg://}"
fi

if [[ -n "${ALEMBIC_DATABASE_URL:-}" ]] && [[ "${ALEMBIC_DATABASE_URL}" == postgres://* ]]; then
  export ALEMBIC_DATABASE_URL="${ALEMBIC_DATABASE_URL/postgres:\/\//postgresql://}"
elif [[ -n "${DATABASE_URL:-}" ]] && [[ -z "${ALEMBIC_DATABASE_URL:-}" ]]; then
  export ALEMBIC_DATABASE_URL="${DATABASE_URL/postgresql+asyncpg:\/\//postgresql://}"
fi

echo "Applying database migrations..."
alembic upgrade head

echo "Starting Uvicorn..."
uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}

