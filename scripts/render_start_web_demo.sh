#!/usr/bin/env sh
set -eu

PORT="${PORT:-8501}"
export HOME="${HOME:-/tmp}"

python_bin="/app/.venv/bin/python"

echo "Starting Streamlit web demo on port ${PORT}..."
exec "$python_bin" -m streamlit run demo/web_demo.py \
  --server.address 0.0.0.0 \
  --server.port "$PORT" \
  --server.headless true \
  --browser.gatherUsageStats false
