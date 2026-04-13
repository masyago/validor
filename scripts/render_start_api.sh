#!/usr/bin/env sh
set -eu

# Render sets PORT for web services.
PORT="${PORT:-8000}"

# Streamlit (and some libs) want a writable HOME.
export HOME="${HOME:-/tmp}"

echo "Running Alembic migrations (with retries)..."
max_retries=30
sleep_seconds=2

attempt=1
while [ "$attempt" -le "$max_retries" ]; do
  if alembic upgrade head; then
    echo "Migrations complete."
    break
  fi

  echo "Migration attempt $attempt/$max_retries failed; retrying in ${sleep_seconds}s..." >&2
  attempt=$((attempt + 1))
  sleep "$sleep_seconds"
done

if [ "$attempt" -gt "$max_retries" ]; then
  echo "Migrations failed after ${max_retries} attempts." >&2
  exit 1
fi

echo "Starting API on port ${PORT}..."
exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
