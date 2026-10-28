#!/usr/bin/env bash
# =============================================================================
# Kiểm tra sức khỏe toàn bộ hệ thống bằng MỘT lệnh (9 nhóm):
#
#     bash scripts/kiem_tra.sh
#
# In ra từng mục ✅ (đạt) / ⚠️ (cần để ý) / ❌ (lỗi) kèm gợi ý sửa.
# Chạy khi đã bật:  docker compose --profile realtime up -d
# =============================================================================

PASS=0; WARN=0; FAIL=0
ok()   { echo "  ✅ $*"; PASS=$((PASS + 1)); }
warn() { echo "  ⚠️  $*"; WARN=$((WARN + 1)); }
bad()  { echo "  ❌ $*"; FAIL=$((FAIL + 1)); }
hdr()  { echo; echo "── $* ──────────────────────────────────────────"; }

# Chạy một câu SQL trên Trino, trả về dòng kết quả cuối (rỗng nếu lỗi).
# Nếu lỗi: giữ lại thông báo lỗi của Trino để in ở cuối (thay vì chỉ thấy dấu "?").
ERRF=$(mktemp); LASTERR=$(mktemp); trap 'rm -f "$ERRF" "$LASTERR"' EXIT
q() {
  local out
  out=$(docker exec trino trino --output-format TSV --execute "$1" 2>"$ERRF" | tail -n 1)
  if [ -n "$out" ]; then
    echo "$out"
  else
    { echo "  SQL : $1"; grep -v -E "jline|Unable to create a system terminal" "$ERRF" | head -n 3 | sed 's/^/  Loi : /'; } > "$LASTERR"
  fi
}
is_num() { [[ "$1" =~ ^-?[0-9]+$ ]]; }

# -----------------------------------------------------------------------------
hdr "1. Container"
for c in object-store nessie-db nessie kafka trino producer bronze-stream silver-stream; do
  info=$(docker inspect -f '{{.State.Status}} {{.RestartCount}} {{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}' "$c" 2>/dev/null)
  if [ -z "$info" ]; then
    bad "$c: chưa được tạo  →  docker compose --profile realtime up -d"; continue
  fi
  read -r status restarts health <<< "$info"
  if [ "$status" != "running" ]; then
    bad "$c: $status  →  docker logs --tail 50 $c"
  elif [ "$health" = "unhealthy" ]; then
    bad "$c: đang chạy nhưng unhealthy  →  docker logs --tail 50 $c"
  elif [ "$restarts" -gt 3 ]; then
    warn "$c: đã tự khởi động lại $restarts lần (có thể đang lỗi lặp)  →  docker logs --tail 80 $c"
  else
    if [ "$health" = "-" ]; then ok "$c: running"; else ok "$c: running ($health)"; fi
  fi
done

# -----------------------------------------------------------------------------
hdr "2. Kafka"
offsets=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic transactions 2>/dev/null \
  | awk -F: '{s += $3} END {print s + 0}')
if is_num "$offsets" && [ "$offsets" -gt 0 ]; then
  ok "topic transactions có $offsets tin"
else
  bad "topic transactions chưa có tin nào  →  docker logs --tail 20 producer"
fi
dlq_offsets=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic dlq_transactions 2>/dev/null \
  | awk -F: '{s += $3} END {print s + 0}')
if is_num "$dlq_offsets" && [ "$dlq_offsets" -gt 0 ]; then
  ok "topic dlq_transactions có $dlq_offsets tin lỗi"
else
  warn "topic dlq_transactions chưa có tin (bình thường trong 1–2 phút đầu)"
fi

# -----------------------------------------------------------------------------
hdr "3. Log của các job (5 phút gần nhất)"
for c in bronze-stream silver-stream; do
  n=$(docker logs --since 5m "$c" 2>&1 | grep -c -E "Exception|Thieu PII_HMAC_KEY")
  last=$(docker logs --since 5m "$c" 2>&1 | grep -E "^\[(bronze|silver)\]" | tail -n 1)
  if [ "$n" -gt 0 ]; then
    bad "$c: có $n dòng lỗi  →  docker logs --tail 80 $c"
  elif [ -n "$last" ]; then
    ok "$c: không có lỗi. Dòng cuối: $last"
  elif docker logs producer 2>&1 | grep -q "Da phat het file"; then
    ok "$c: không có lỗi, đang chờ tin mới (producer đã phát hết file)"
  else
    warn "$c: chưa in tiến độ trong 5 phút qua (vừa khởi động?)"
  fi
done

# -----------------------------------------------------------------------------
hdr "4. Dữ liệu trong lakehouse (truy vấn qua Trino)"
bronze=$(q "SELECT count(*) FROM iceberg.bronze.transactions_raw")
silver=$(q "SELECT count(*) FROM iceberg.silver.transactions")
dlq=$(q "SELECT count(*) FROM iceberg.bronze.dlq")
if ! is_num "$bronze"; then
  bad "Trino không đọc được bảng Bronze  →  docker logs --tail 50 trino (Trino cần ~1 phút sau khi bật)"
else
  if [ "$bronze" -gt 0 ]; then ok "Bronze: $bronze bản ghi | Silver: ${silver:-?} | DLQ: ${dlq:-?}"
  else warn "Bronze chưa có bản ghi nào (đợi 1–2 phút sau khi bật realtime)"; fi

  fresh_b=$(q "SELECT CAST(to_unixtime(current_timestamp) - to_unixtime(max(ingest_time)) AS bigint) FROM iceberg.bronze.transactions_raw")
  fresh_s=$(q "SELECT CAST(to_unixtime(current_timestamp) - to_unixtime(max(silver_time)) AS bigint) FROM iceberg.silver.transactions")
  for pair in "Bronze:$fresh_b" "Silver:$fresh_s"; do
    name=${pair%%:*}; sec=${pair#*:}
    if ! is_num "$sec"; then bad "$name: không đo được độ tươi"
    elif [ "$sec" -le 120 ]; then ok "$name: mẻ mới nhất cách đây $sec giây (dữ liệu đang chảy)"
    else warn "$name: mẻ mới nhất cách đây $sec giây — job hoặc producer có thể đã dừng"
    fi
  done
fi

# -----------------------------------------------------------------------------
hdr "5. Chất lượng dữ liệu"
v=$(q "SELECT count(*) FROM iceberg.bronze.transactions_raw WHERE sim_scenario = 'invalid'")
if is_num "$v" && [ "$v" -eq 0 ]; then ok "Bronze không lẫn bản ghi lỗi"
else bad "Bronze có ${v:-?} bản ghi lỗi lọt vào (luật kiểm tra hỏng?)"; fi

v=$(q "SELECT count(DISTINCT error_reason) FROM iceberg.bronze.dlq")
if is_num "$v" && [ "$v" -ge 1 ]; then ok "DLQ bắt được $v loại lỗi"
else warn "DLQ chưa có bản ghi (bình thường trong vài phút đầu)"; fi

v=$(q "SELECT count(*) - count(DISTINCT transaction_id) FROM iceberg.bronze.transactions_raw")
if is_num "$v" && [ "$v" -gt 0 ]; then ok "Bronze có $v bản trùng (producer cố ý gửi trùng — đúng như mong đợi)"
else warn "Bronze chưa thấy bản trùng nào (đợi thêm vài phút)"; fi

v=$(q "SELECT count(*) - count(DISTINCT transaction_id) FROM iceberg.silver.transactions")
if is_num "$v" && [ "$v" -eq 0 ]; then ok "Silver không có bản trùng"
else bad "Silver có ${v:-?} bản trùng (bỏ trùng chưa hoạt động)"; fi

# -----------------------------------------------------------------------------
hdr "6. Bảo mật PII ở Silver"
v=$(q "SELECT count(*) FROM iceberg.information_schema.columns WHERE table_schema = 'silver' AND table_name = 'transactions' AND column_name IN ('card_number', 'first_name', 'last_name', 'street', 'zip', 'dob', 'cust_lat', 'cust_long', 'job')")
if is_num "$v" && [ "$v" -eq 0 ]; then ok "Silver không còn cột PII (số thẻ, họ tên, địa chỉ, ngày sinh, tọa độ nhà)"
else bad "Silver vẫn còn ${v:-?} cột PII"; fi

v=$(q "SELECT count(*) FROM iceberg.silver.transactions WHERE length(card_token) <> 64 OR starts_with(merchant, 'fraud_')")
if is_num "$v" && [ "$v" -eq 0 ]; then ok "Mọi card_token dài 64 ký tự, tên merchant đã bỏ tiền tố fraud_"
else bad "Có ${v:-?} dòng token/merchant sai định dạng"; fi

v=$(q "SELECT (SELECT count(DISTINCT card_number) FROM iceberg.bronze.transactions_raw) - (SELECT count(DISTINCT card_token) FROM iceberg.silver.transactions)")
if is_num "$v" && [ "${v#-}" -le 5 ]; then ok "Số thẻ ở Bronze ≈ số token ở Silver (lệch $v): cùng thẻ → cùng token"
elif is_num "$v"; then warn "Số thẻ Bronze và số token Silver lệch $v — Silver có thể đang chạy chậm hơn Bronze"; fi

# -----------------------------------------------------------------------------
hdr "7. Dữ liệu lịch sử (backfill)"
v=$(q "SELECT count(*) FROM iceberg.silver.transactions WHERE ingest_source = 'batch'")
if is_num "$v" && [ "$v" -gt 0 ]; then
  ok "Silver có $v giao dịch lịch sử nạp từ fraudTrain.csv"
else
  warn "Chưa nạp lịch sử  →  docker compose --profile jobs run --rm backfill"
fi
v=$(q "SELECT count(*) FROM iceberg.silver.transactions WHERE is_fraud = 1")
if is_num "$v" && [ "$v" -gt 0 ]; then ok "Silver có $v giao dịch gian lận có nhãn (dùng ở Giai đoạn 3)"; fi

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
hdr "8. Tầng Gold và chấm điểm gian lận (Giai đoạn 3)"
v=$(q "SELECT count(*) FROM iceberg.information_schema.tables WHERE table_schema = 'gold'")
if ! is_num "$v" || [ "$v" -eq 0 ]; then
  warn "Chưa có tầng Gold  →  docker compose --profile jobs run --rm gold"
else
  g=$(q "SELECT count(*) FROM iceberg.gold.transaction_features")
  s=$(q "SELECT count(*) FROM iceberg.silver.transactions")
  if is_num "$g" && is_num "$s" && [ "$g" -eq "$s" ]; then
    ok "Gold có đủ $g giao dịch (bằng Silver)"
  elif is_num "$g" && is_num "$s"; then
    warn "Gold có $g giao dịch, Silver có $s — Silver đã có thêm dữ liệu, chạy lại job gold để cập nhật"
  fi
  r=$(q "SELECT concat(format('%.3f', precision), ' ', format('%.3f', recall), ' ', format('%.3f', coalesce(roc_auc, 0)), ' ', cast(alerts AS varchar)) FROM iceberg.gold.model_metrics WHERE split = 'test' AND is_chosen AND run_time = (SELECT max(run_time) FROM iceberg.gold.model_metrics)")
  if [ -n "$r" ]; then
    read -r p rc auc al <<< "$r"
    ok "Kết quả trên TEST lần chạy mới nhất: precision $p · recall $rc · ROC-AUC $auc · $al cảnh báo"
  else
    warn "Chưa có kết quả đánh giá trên TEST (producer đã phát fraudTest chưa?)"
  fi
  v=$(q "SELECT count(*) FROM iceberg.gold.fraud_alerts WHERE cardinality(reasons) = 0")
  if is_num "$v" && [ "$v" -eq 0 ]; then ok "Mọi cảnh báo đều có lý do đi kèm"
  elif is_num "$v"; then warn "$v cảnh báo không có lý do (ngưỡng điểm quá thấp?)"; fi
fi

# -----------------------------------------------------------------------------
hdr "9. dbt, bảo trì Iceberg, Airflow (Giai đoạn 4)"
v=$(q "SELECT count(*) FROM iceberg.information_schema.tables WHERE table_schema = 'marts'")
if is_num "$v" && [ "$v" -ge 3 ]; then ok "dbt đã dựng $v bảng báo cáo trong iceberg.marts"
else warn "Chưa có bảng dbt  →  docker compose --profile jobs run --rm dbt"; fi

v=$(q "SELECT count(*) FROM iceberg.information_schema.tables WHERE table_schema = 'ops' AND table_name = 'maintenance_log'")
if is_num "$v" && [ "$v" -eq 1 ]; then
  r=$(q "SELECT concat(cast(files_before AS varchar), ' ', cast(files_after AS varchar), ' ', cast(query_sec_before AS varchar), ' ', cast(query_sec_after AS varchar)) FROM iceberg.ops.maintenance_log WHERE table_name = 'lakehouse.bronze.transactions_raw' ORDER BY run_time DESC LIMIT 1")
  if [ -n "$r" ]; then
    read -r fb fa qb qa <<< "$r"
    ok "Lần bảo trì gần nhất (Bronze): $fb → $fa file, truy vấn $qb s → $qa s"
  fi
else
  warn "Chưa chạy bảo trì Iceberg  →  docker compose --profile jobs run --rm maintenance"
fi

# Airflow riêng của dự án (container "airflow") hoặc Airflow có sẵn trên máy (…scheduler…)
af=$(docker ps --format '{{.Names}}' | grep -E '^airflow$|airflow.*scheduler' | head -n 1)
if [ -z "$af" ]; then
  warn "Không thấy Airflow đang chạy (tùy chọn)  →  bật Airflow của bạn rồi: bash scripts/noi_airflow_co_san.sh"
else
  n=$(docker exec "$af" airflow dags list 2>/dev/null | grep -c 'lakehouse_')
  if is_num "$n" && [ "$n" -ge 3 ]; then ok "Airflow ($af) có đủ 3 DAG lakehouse"
  else warn "Airflow ($af) chưa thấy DAG lakehouse  →  bash scripts/noi_airflow_co_san.sh"; fi
fi

if [ -s "$LASTERR" ]; then
  echo; echo "── Lỗi Trino gần nhất (nguyên nhân của các dấu ? ở trên) ──"
  cat "$LASTERR"
fi

echo
echo "══ KẾT QUẢ: $PASS đạt · $WARN cần để ý · $FAIL lỗi ══"
if [ "$FAIL" -eq 0 ]; then
  echo "Hệ thống chạy tốt."
else
  echo "Làm theo gợi ý sau dấu → ở các dòng ❌, hoặc gửi toàn bộ kết quả này để được hỗ trợ."
fi
