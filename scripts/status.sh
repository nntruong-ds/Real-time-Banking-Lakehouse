#!/usr/bin/env bash
# =============================================================================
# Xem nhanh tình trạng hệ thống: container, log mới nhất, số bản ghi từng bảng.
#
#   bash scripts/status.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

printf '\n\033[1;34m=== Container ===\033[0m\n'
docker compose --profile realtime ps -a --format 'table {{.Name}}\t{{.Status}}'

printf '\n\033[1;34m=== Dong log moi nhat cua tung job ===\033[0m\n'
for c in producer bronze-stream silver-stream; do
  line=$(docker logs "$c" 2>&1 | grep -E '^\[(replay|bronze|silver|pii)\]' | tail -1)
  err=$(docker logs --tail 200 "$c" 2>&1 | grep -E 'Exception|Error:' | tail -1)
  printf '%-14s %s\n' "$c" "${line:-(chua co log)}"
  [[ -n "$err" ]] && printf '%-14s \033[31m%s\033[0m\n' "" "LOI GAN NHAT: $err"
done

printf '\n\033[1;34m=== So ban ghi (truy van bang Trino) ===\033[0m\n'
docker exec trino trino --output-format ALIGNED --execute "
SELECT 'bronze.transactions_raw' AS bang, count(*) AS so_ban_ghi FROM iceberg.bronze.transactions_raw
UNION ALL SELECT 'bronze.dlq',          count(*) FROM iceberg.bronze.dlq
UNION ALL SELECT 'silver.transactions', count(*) FROM iceberg.silver.transactions
" 2>&1 | grep -v WARNING || echo "Chua truy van duoc (bang chua duoc tao hoac Trino chua san sang)."

echo
echo "Chay lai lenh nay sau 30 giay: so ban ghi phai tang len."
