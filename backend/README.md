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

5. Import the portfolio dataset:

```bash
python scripts/import_dataset.py /home/abdelhadi/Downloads/all_in_one_dataset.csv
```

Main starter endpoints after import:

- `GET /health`
- `GET /docs`
- `GET /api/v1/policies`
- `GET /api/v1/policies/summary`
- `GET /api/v1/geo/wilayas`
- `GET /api/v1/geo/wilayas/{code}/communes`
- `GET /api/v1/geo/zone/{wilaya_code}/{commune_name}`

## Render

- Build command: `pip install --upgrade pip && pip install -r requirements.txt`
- Start command: `bash scripts/start.sh`
