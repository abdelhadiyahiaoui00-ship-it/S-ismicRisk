# RASED Backend

Foundation scaffold for the FastAPI backend:

- FastAPI + Uvicorn
- Async SQLAlchemy
- Alembic migrations
- PostgreSQL
- Docker / Docker Compose
- Render deployment config

## Local development

1. Copy env file:

```bash
cp .env.example .env
```

2. Start PostgreSQL:

```bash
docker compose up -d
```

3. Install dependencies and run migrations:

```bash
pip install -r requirements.txt
alembic upgrade head
```

4. Run the API:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Render

- Build command: `pip install --upgrade pip && pip install -r requirements.txt`
- Start command: `bash scripts/start.sh`

