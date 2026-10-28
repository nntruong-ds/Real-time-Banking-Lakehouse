#!/usr/bin/env bash
# Khởi động Superset và nạp dashboard từ code (thư mục /app/dashboards = superset/ trong repo).
set -euo pipefail

echo "[superset] Cap nhat metadata..."
superset db upgrade

if ! superset fab list-users 2>/dev/null | grep -q "username:${SUPERSET_ADMIN_USER}"; then
  echo "[superset] Tao tai khoan ${SUPERSET_ADMIN_USER}..."
  superset fab create-admin --username "${SUPERSET_ADMIN_USER}" --firstname Admin \
    --lastname Lakehouse --email admin@lakehouse.local --password "${SUPERSET_ADMIN_PASSWORD}"
fi
superset init

echo "[superset] Nap dashboard tu code (ban trong git luon thang)..."
if python /app/import_dashboards.py /app/dashboards; then
  echo "[superset] Da nap dashboard."
else
  echo "[superset] LOI khi nap dashboard (xem log o tren). Superset van chay."
fi

echo "[superset] San sang: http://localhost:8088"
exec /app/docker/entrypoints/run-server.sh
