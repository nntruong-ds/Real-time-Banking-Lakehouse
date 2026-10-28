#!/usr/bin/env bash
# =============================================================================
# NỐI dự án lakehouse vào một Airflow ĐÃ CÓ SẴN trên máy (Airflow 2.4+ hoặc 3.x chạy bằng
# docker compose), thay vì bật Airflow riêng của dự án.
#
#     bash scripts/noi_airflow_co_san.sh          # xem trước rồi hỏi xác nhận
#     bash scripts/noi_airflow_co_san.sh --go     # bỏ qua câu hỏi
#
# Script KHÔNG sửa file nào của dự án Airflow kia. Nó chỉ thêm một file
#     <thư mục dự án Airflow>/docker-compose.override.yml
# (docker compose tự gộp file này vào cấu hình gốc) rồi khởi động lại Airflow. File đó cho Airflow:
#   1. thấy 3 DAG lakehouse    : gắn airflow/dags của dự án này vào <dags_folder>/lakehouse
#   2. quyền dùng Docker        : gắn /var/run/docker.sock + thêm vào nhóm docker
#   3. biết dự án nằm ở đâu      : HOST_PROJECT_DIR, và đọc PII_HMAC_KEY từ .env của dự án này
#
# GỠ BỎ: xóa file override đó rồi chạy  docker compose up -d  trong thư mục dự án Airflow.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1
LAKEHOUSE_DIR="$(pwd)"

say()  { echo "-> $*"; }
fail() { echo "LOI: $*" >&2; exit 1; }

# --------------------------------------------------------------------- 1. tìm Airflow
[ -f .env ] || fail "Khong thay file .env trong $LAKEHOUSE_DIR (can PII_HMAC_KEY)."
grep -q '^PII_HMAC_KEY=.\{32,\}' .env || fail "File .env chua co PII_HMAC_KEY (>= 32 ky tu)."

SCHED=$(docker ps --format '{{.Names}}' | grep -i 'airflow' | grep -i 'scheduler' | head -n 1 || true)
[ -n "$SCHED" ] || fail "Khong thay container Airflow scheduler nao dang chay. Bat Airflow cua ban truoc."

label() { docker inspect -f "{{ index .Config.Labels \"$1\" }}" "$SCHED"; }
PROJECT=$(label com.docker.compose.project)
WORKDIR=$(label com.docker.compose.project.working_dir)
CONFIG_FILES=$(label com.docker.compose.project.config_files)
[ -n "$PROJECT" ] && [ -d "$WORKDIR" ] || \
  fail "$SCHED khong duoc tao bang docker compose (hoac thu muc $WORKDIR khong con). Script chi ho tro Airflow chay bang docker compose."

VERSION=$(docker exec "$SCHED" airflow version 2>/dev/null | tail -n 1)
EXECUTOR=$(docker exec "$SCHED" airflow config get-value core executor 2>/dev/null | tail -n 1)
DAGS_FOLDER=$(docker exec "$SCHED" airflow config get-value core dags_folder 2>/dev/null | tail -n 1)
DOCKER_PROVIDER=$(docker exec "$SCHED" python -c \
  "import airflow.providers.docker as p; print(getattr(p, '__version__', 'co'))" 2>/dev/null || true)
DOCKER_GID=$(getent group docker | cut -d: -f3)

case "$VERSION" in
  2.[4-9]*|2.1[0-9]*|3.*) ;;
  *) fail "Airflow $VERSION qua cu (can 2.4 tro len)." ;;
esac
[ -n "$DOCKER_PROVIDER" ] || fail "Airflow cua ban chua cai apache-airflow-providers-docker."
[ -n "$DOCKER_GID" ] || fail "May chua co nhom 'docker'."
case "$CONFIG_FILES" in
  *docker-compose.yml|*docker-compose.yaml|*compose.yml|*compose.yaml) ;;
  *) fail "Du an Airflow dung file cau hinh ten la ($CONFIG_FILES): docker compose se KHONG tu gop docker-compose.override.yml. Hay noi cach thu cong (README, muc Airflow co san)." ;;
esac

# Các service Airflow đang chạy trong dự án đó (bỏ postgres, redis...)
SERVICES=()
for c in $(docker ps --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}}'); do
  if docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$c" | grep -q '^AIRFLOW__'; then
    SERVICES+=("$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.service" }}' "$c")")
  fi
done
[ ${#SERVICES[@]} -gt 0 ] || fail "Khong tim thay service Airflow nao trong du an $PROJECT."

OVERRIDE="$WORKDIR/docker-compose.override.yml"
[ -w "$WORKDIR" ] || fail "Khong co quyen ghi vao $WORKDIR. Chay: sudo chown $(id -un) \"$WORKDIR\"  roi chay lai script."
if [ -f "$OVERRIDE" ] && ! grep -q "realtime-banking-lakehouse" "$OVERRIDE"; then
  fail "$OVERRIDE da ton tai (cua ban). Khong ghi de. Hay noi cach thu cong (README, muc Airflow co san)."
fi

echo
echo "  Airflow      : $VERSION ($EXECUTOR), provider docker $DOCKER_PROVIDER"
echo "  Du an        : $PROJECT  ->  $WORKDIR"
echo "  Service      : ${SERVICES[*]}"
echo "  DAG se o     : $DAGS_FOLDER/lakehouse/lakehouse_dags.py"
echo "  Se ghi file  : $OVERRIDE"
echo "  Roi chay     : (cd $WORKDIR && docker compose up -d)   <- Airflow khoi dong lai ~1 phut"
echo
if [ "${1:-}" != "--go" ]; then
  read -r -p "Tiep tuc? Go yes de dong y: " ans
  [ "$ans" = "yes" ] || { echo "Da huy, khong thay doi gi."; exit 0; }
fi

# --------------------------------------------------------------------- 2. ghi override
# Thư mục đích của DAG nằm trong dags folder; nếu dags folder là thư mục trên máy thì tạo sẵn
# thư mục con 'lakehouse' bằng quyền của bạn (để Docker không tạo nó bằng quyền root).
# Nếu không tạo được (thư mục dags do Docker tạo, thuộc root) thì bỏ qua: Docker sẽ tự tạo.
DAGS_HOST=$(docker inspect -f "{{range .Mounts}}{{if eq .Destination \"$DAGS_FOLDER\"}}{{.Source}}{{end}}{{end}}" "$SCHED")
if [ -n "$DAGS_HOST" ] && [ ! -d "$DAGS_HOST/lakehouse" ]; then
  mkdir -p "$DAGS_HOST/lakehouse" 2>/dev/null || \
    say "Khong tao duoc $DAGS_HOST/lakehouse (thu muc thuoc nguoi dung khac) - Docker se tu tao, khong sao."
fi

{
  echo "# Tao boi realtime-banking-lakehouse/scripts/noi_airflow_co_san.sh"
  echo "# Go bo: xoa file nay roi chay 'docker compose up -d' trong thu muc nay."
  echo "services:"
  for s in "${SERVICES[@]}"; do
    cat <<EOF
  $s:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - $LAKEHOUSE_DIR/airflow/dags:$DAGS_FOLDER/lakehouse:ro
    group_add:
      - "$DOCKER_GID"
    environment:
      HOST_PROJECT_DIR: $LAKEHOUSE_DIR
    env_file:
      - $LAKEHOUSE_DIR/.env
EOF
  done
} > "$OVERRIDE"
say "Da ghi $OVERRIDE"

(cd "$WORKDIR" && docker compose config --quiet) || fail "Cau hinh sau khi gop bi loi. Xoa $OVERRIDE de quay lai nhu cu."

# --------------------------------------------------------------------- 3. khởi động lại
say "Khoi dong lai Airflow ($PROJECT)..."
(cd "$WORKDIR" && docker compose up -d)

say "Doi Airflow nap DAG (toi da 3 phut)..."
for _ in $(seq 1 36); do
  sleep 5
  n=$(docker exec "$SCHED" airflow dags list 2>/dev/null | grep -c 'lakehouse_' || true)
  [ "${n:-0}" -ge 3 ] && break
done

echo
if [ "${n:-0}" -ge 3 ]; then
  say "XONG: Airflow da thay 3 DAG lakehouse_daily, lakehouse_backfill, lakehouse_maintenance."
  say "Mo giao dien Airflow cua ban, tim tag 'lakehouse'."
else
  echo "Chua thay du 3 DAG. Loi nap DAG (neu co):"
  docker exec "$SCHED" airflow dags list-import-errors 2>/dev/null || true
  echo "Gui toan bo ket qua nay de duoc ho tro."
  exit 1
fi
