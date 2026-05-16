# Tallent Management Portal (Local + Neon)

This copy is prepared to run locally against Neon/Postgres.

## Deploy on Railway

1. Push this folder to a GitHub repo.
2. In Railway: **New Project** -> **Deploy from GitHub Repo**.
3. Set environment variables in Railway:
   - `DATABASE_URL` = your Neon connection string (with `sslmode=require`)
   - Optional: `APP_TABLE_FQN` (default `public.tallent_training_register`)
4. Railway will start with `Procfile` (`web: python app.py`).
5. Open the generated Railway URL.

## Run locally with Neon

1. Set environment variable:

```bash
export DATABASE_URL="postgresql://<user>:<password>@<host>/<db>?sslmode=require"
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run app:

```bash
python app.py
```

4. Open:

```text
http://127.0.0.1:8000
```

## Notes

- In local Neon mode (`DATABASE_URL` set), data is stored in:
  - `public.tallent_training_register`
- Without `DATABASE_URL`, app uses Databricks SQL warehouse mode.
