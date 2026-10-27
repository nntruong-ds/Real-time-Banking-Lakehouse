#!/usr/bin/env bash
# =============================================================================
# XÓA SẠCH mọi container, dữ liệu, catalog và checkpoint của dự án
# (kể cả đồ do các phiên bản cũ để lại: spark-iceberg, iceberg-rest, minio...).
#
#   bash scripts/reset.sh            # giữ lại image đã build (lần sau khỏi build lại)
#   bash scripts/reset.sh --images   # xóa luôn image của dự án
#
# KHÔNG đụng tới code, file .env và thư mục datasets/.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

read -r -p "Xoa TOAN BO du lieu cua du an (Kafka, bang Iceberg, catalog, checkpoint)? Go yes de xac nhan: " ans
[[ "$ans" == "yes" ]] || { echo "Da huy, khong xoa gi."; exit 0; }

echo "-> Tat va xoa container + volume cua phien ban hien tai"
docker compose --profile realtime --profile jobs down -v --remove-orphans

echo "-> Xoa container con sot lai tu cac phien ban cu"
for c in kafka object-store minio create-bucket iceberg-rest nessie nessie-db \
         spark-iceberg trino producer bronze-stream silver-stream spark-tools backfill; do
  docker rm -f "$c" >/dev/null 2>&1 && echo "   da xoa container $c"
done

echo "-> Xoa volume con sot lai"
docker volume ls -q | grep -E '^realtime-banking-lakehouse[0-9]*_' | while read -r v; do
  docker volume rm "$v" >/dev/null && echo "   da xoa volume $v"
done

docker network rm lakehouse >/dev/null 2>&1 && echo "-> Da xoa network lakehouse"

if [[ "${1:-}" == "--images" ]]; then
  echo "-> Xoa image cua du an"
  docker images --format '{{.Repository}}:{{.Tag}}' | grep -E '^lakehouse-' | xargs -r docker rmi -f
fi

echo
echo "Xong. Bat lai tu dau bang:  bash scripts/start.sh"
