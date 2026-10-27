#!/usr/bin/env bash
# =============================================================================
# Bật TOÀN BỘ dự án bằng một lệnh, kiểm tra từng bước, dừng lại báo lỗi rõ ràng.
#
#   bash scripts/start.sh
#
# Chạy lại bao nhiêu lần cũng được: bước nào đã xong thì chỉ kiểm tra lại.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n\033[1;34m=== %s ===\033[0m\n' "$*"; }
ok()   { printf '\033[32m[OK]\033[0m %s\n' "$*"; }
fail() { printf '\n\033[1;31m[LOI]\033[0m %s\n' "$*" >&2; exit 1; }

# Chờ container báo "healthy" (dựa vào healthcheck khai báo trong docker-compose.yml)
wait_healthy() {
  local name=$1 timeout=${2:-180} waited=0 status
  while :; do
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$name" 2>/dev/null || echo "missing")
    case "$status" in
      healthy|running) ok "$name: $status"; return 0 ;;
      unhealthy|exited|dead|missing)
        docker logs --tail 30 "$name" 2>&1 || true
        fail "$name o trang thai '$status'. Log o tren." ;;
    esac
    if (( waited >= timeout )); then
      docker logs --tail 30 "$name" 2>&1 || true
      fail "$name chua san sang sau ${timeout}s (trang thai: $status)."
    fi
    sleep 5; waited=$((waited + 5))
  done
}

# Chờ một dòng log xác nhận job streaming đã xử lý xong ít nhất một mẻ
wait_log() {
  local name=$1 pattern=$2 timeout=${3:-300} waited=0
  while ! docker logs "$name" 2>&1 | grep -qE "$pattern"; do
    if [[ "$(docker inspect -f '{{.State.Restarting}}' "$name" 2>/dev/null)" == "true" ]] \
       || (( waited >= timeout )); then
      echo "---- 40 dong log cuoi cua $name ----"
      docker logs --tail 40 "$name" 2>&1 | grep -vE ' (INFO|WARN) ' || true
      fail "$name chua chay duoc. Gui phan log o tren de duoc ho tro."
    fi
    sleep 5; waited=$((waited + 5))
  done
  ok "$name: $(docker logs "$name" 2>&1 | grep -E "$pattern" | tail -1)"
}

step "1/6 Kiem tra Docker"
command -v docker >/dev/null || fail "Chua cai Docker. Xem Buoc 0 trong README."
docker info >/dev/null 2>&1 || fail "Khong goi duoc Docker. Thu: sudo systemctl start docker
   Neu bao 'permission denied': sudo usermod -aG docker \$USER, roi dang xuat va dang nhap lai."
docker compose version >/dev/null 2>&1 || fail "Thieu Docker Compose. Xem Buoc 0 trong README."
ok "$(docker --version)"

step "2/6 Khoa bi mat .env (dung de bam so the)"
if [[ -f .env ]] && grep -qE '^PII_HMAC_KEY=[0-9a-fA-F]{32,}$' .env; then
  ok ".env da co, giu nguyen khoa cu"
else
  command -v openssl >/dev/null || fail "Thieu openssl: sudo apt install -y openssl"
  echo "PII_HMAC_KEY=$(openssl rand -hex 32)" > .env
  ok "Da tao .env voi khoa moi (dung xoa file nay)"
fi

if [[ ! -f datasets/sparkov/fraudTest.csv || ! -f datasets/sparkov/fraudTrain.csv ]]; then
  fail "Chua co dataset. Tai tu https://www.kaggle.com/datasets/kartik2112/fraud-detection
   roi giai nen:  mkdir -p datasets/sparkov && unzip -o ~/Downloads/archive.zip -d datasets/sparkov"
fi
ok "Dataset Sparkov da co trong datasets/sparkov"

step "3/6 Build image (lan dau 5-15 phut, cac lan sau vai giay)"
docker compose --profile realtime --profile jobs build
ok "Image da san sang"

step "4/6 Bat ha tang: kho S3, Postgres, Nessie, Kafka, Trino"
docker compose up -d
for c in object-store nessie-db nessie kafka trino; do wait_healthy "$c" 240; done

step "5/6 Chay unit test"
docker compose --profile jobs run --rm spark-tools || fail "Unit test that bai. Xem log o tren."
ok "Unit test deu qua"

step "6/6 Bat luong realtime: producer, bronze-stream, silver-stream"
docker compose --profile realtime up -d
wait_log producer       '^\[replay\] (da gui|Da phat het)'  120
wait_log bronze-stream  '^\[bronze\] bronze_valid me #'  300
wait_log silver-stream  '^\[silver\] silver +me #'  300

printf '\n\033[1;32m=== XONG: toan bo he thong dang chay ===\033[0m\n'
cat <<'EOF'

Kiem tra toan bo he thong:         bash scripts/kiem_tra.sh
Nap du lieu lich su (1 lan):       docker compose --profile jobs run --rm backfill
Xem nhanh so ban ghi:              bash scripts/status.sh
Vao Trino de go SQL:               docker exec -it trino trino
Xem log mot job:                   docker logs -f silver-stream     (Ctrl + C de thoat)
Tam dung tat ca:                   docker compose --profile realtime stop
Xoa sach lam lai tu dau:           bash scripts/reset.sh

Giao dien web:
  Nessie   http://localhost:19120
  Kho S3   http://localhost:9001   (admin / password)
  Trino    http://localhost:8085
  Spark    http://localhost:4040 (Bronze), http://localhost:4041 (Silver)
EOF
