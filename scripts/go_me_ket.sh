#!/usr/bin/env bash
# =============================================================================
# GỠ MẺ STREAMING BỊ KẸT (mẻ quá lớn, lần nào chạy cũng tràn bộ nhớ).
#
#   bash scripts/go_me_ket.sh                                   # query bronze_valid
#   bash scripts/go_me_ket.sh silver-stream silver_from_kafka   # query của Silver
#
# Vì sao kẹt: trước khi chạy một mẻ, Spark ghi KẾ HOẠCH mẻ đó (đọc từ offset nào tới offset
# nào) vào checkpoint/offsets/<số mẻ>. Mẻ chạy xong mới ghi checkpoint/commits/<số mẻ>.
# Job chết giữa chừng -> khởi động lại sẽ chạy lại ĐÚNG kế hoạch cũ, kể cả khi bạn đã giảm
# maxOffsetsPerTrigger. Kế hoạch cũ quá lớn -> tràn bộ nhớ mãi mãi.
#
# Script này xóa kế hoạch của mẻ CHƯA hoàn thành (có offsets/N nhưng không có commits/N).
# Spark sẽ lập lại kế hoạch mẻ N với giới hạn mới, bắt đầu từ đúng chỗ mẻ N-1 đã dừng,
# nên KHÔNG mất và KHÔNG trùng dữ liệu. Mẻ đã hoàn thành thì không bao giờ bị đụng tới.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1

SERVICE=${1:-bronze-stream}
CHECKPOINT=${2:-bronze_transactions}

if ! docker inspect "$SERVICE" >/dev/null 2>&1; then
  echo "Khong thay container $SERVICE. Bat truoc: docker compose --profile realtime up -d"
  exit 1
fi
VOLUME=$(docker inspect -f \
  '{{range .Mounts}}{{if eq .Destination "/opt/spark/checkpoints"}}{{.Name}}{{end}}{{end}}' "$SERVICE")
IMAGE=$(docker inspect -f '{{.Config.Image}}' "$SERVICE")

echo "-> Tat $SERVICE (checkpoint nam trong volume $VOLUME)"
docker compose --profile realtime stop "$SERVICE"

echo "-> Kiem tra checkpoint $CHECKPOINT"
docker run --rm -u 0 -v "$VOLUME":/cp --entrypoint bash "$IMAGE" -c '
  d=/cp/'"$CHECKPOINT"'
  if [ ! -d "$d/offsets" ]; then echo "   Chua co checkpoint $d -> khong co gi de go."; exit 0; fi
  last_off=$(ls "$d/offsets" | grep -E "^[0-9]+$" | sort -n | tail -n 1)
  last_com=$(ls "$d/commits" 2>/dev/null | grep -E "^[0-9]+$" | sort -n | tail -n 1 || true)
  echo "   Me da len ke hoach gan nhat: #${last_off:-khong co} | me da hoan thanh gan nhat: #${last_com:-khong co}"
  if [ -n "$last_off" ] && [ "$last_off" -gt "${last_com:--1}" ]; then
    rm -f "$d/offsets/$last_off" "$d/offsets/.$last_off.crc"
    echo "   Da xoa ke hoach me #$last_off (chua hoan thanh). Spark se lap lai me nay voi gioi han moi."
  else
    echo "   Khong co me nao dang ket -> khong xoa gi."
  fi
'

echo "-> Bat lai $SERVICE"
docker compose --profile realtime start "$SERVICE"
echo "Xong. Xem log:  docker logs -f --since 1m $SERVICE"
