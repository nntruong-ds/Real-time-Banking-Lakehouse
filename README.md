# Real-time Banking Lakehouse & Fraud Scoring

Lakehouse xử lý giao dịch thẻ ngân hàng — vừa **batch** (nạp lịch sử) vừa **streaming** (thời gian thực) — chạy hoàn toàn trên máy local bằng Docker:
**Kafka → Spark Structured Streaming → Apache Iceberg (catalog Nessie, kho S3) → Trino**.

> **Trạng thái: Giai đoạn 4 — dbt, Airflow, bảo trì Iceberg.** Dữ liệu là dataset giao dịch thẻ công khai **Sparkov** (Kaggle, ~1,85 triệu giao dịch, có nhãn gian lận).
> Lịch sử 2019 → 06/2020 nạp bằng job batch; nửa cuối 2020 được phát lại qua Kafka như hệ thống thanh toán đang chạy.
> Tầng Gold tính đặc trưng theo thẻ, chấm điểm bằng **scorecard WoE** học trên lịch sử; trên 6 tháng cuối 2020 mà scorecard chưa từng thấy đạt **precision 0,62 · recall 0,62**.
> **dbt** dựng bảng báo cáo và chạy test đối soát giữa các tầng; **Airflow 3** lên lịch mọi job; job **bảo trì Iceberg** gộp file nhỏ và đo hiệu quả trước/sau.

Hướng dẫn viết cho **Ubuntu 22.04 / 24.04**, máy **16 GB RAM**. Mọi lệnh gõ trong Terminal, đứng trong thư mục dự án.

---

## Kiến trúc

```
 fraudTrain.csv ─► [backfill] (batch, chạy 1 lần) ─────────────┬─► bronze.transactions_raw
 (2019 → 06/2020)          cùng luật kiểm tra, MERGE không trùng │─► bronze.dlq
                                                                └─► silver.transactions
 fraudTest.csv ─► [producer] ─► Kafka ─┬─► [bronze-stream] ─┬─► hợp lệ ─► bronze.transactions_raw  (bản thô, có PII)
 (06 → 12/2020)  phát lại x3600        │                    └─► lỗi ────► bronze.dlq + Kafka topic dlq_transactions
                                       └─► [silver-stream] ──► hợp lệ ─► bỏ trùng · che PII ─► silver.transactions
                                                                                                   │
 silver.transactions ─► [gold] (batch) ─► đặc trưng theo thẻ ─► scorecard WoE ─┬─► gold.transaction_features
                        học trên TRAIN (≤ 21/06/2020), đo trên TEST            ├─► gold.scorecard
                                                                               ├─► gold.fraud_alerts (kèm lý do)
                                                                               └─► gold.model_metrics
                                                                                          │
 [dbt] (SQL trên Trino) ─► marts.fct_daily_fraud · marts.dim_card_risk · marts.agg_category_month
                           + test chất lượng, test ĐỐI SOÁT Bronze↔Silver↔Gold, cổng chất lượng mô hình
 [maintenance] (Spark)  ─► gộp file nhỏ mọi bảng, ghi số file + thời gian truy vấn trước/sau vào ops.maintenance_log
 [Airflow 3] lên lịch:  lakehouse_daily (gold → dbt build) · lakehouse_backfill (18 tháng) · lakehouse_maintenance (hằng tuần)

Bảng Iceberg:  metadata trong Nessie (lưu ở Postgres)  ·  file Parquet trên kho S3 (RustFS)
Truy vấn:      Trino  (catalog "iceberg")
```

| Thành phần | Vai trò | Phiên bản |
| --- | --- | --- |
| Kafka | Hàng đợi giao dịch (KRaft, 1 node) | `apache/kafka:3.9.0` |
| RustFS | Kho lưu file tương thích S3 | `rustfs/rustfs:1.0.0` |
| Nessie + Postgres | Catalog Iceberg có lịch sử phiên bản kiểu git | `nessie:0.108.8`, `postgres:16.4` |
| Spark (image tự build) | Job streaming và batch, có sẵn Iceberg và Kafka connector | Spark 3.5.9, Iceberg 1.10.0 |
| Trino | SQL trên các bảng Iceberg | `trinodb/trino:476` |
| Producer | Phát lại dataset vào Kafka theo nhịp thời gian | Python 3.12 |
| dbt | Bảng báo cáo + test đối soát, chạy SQL trên Trino | `dbt-core 1.11.16`, `dbt-trino 1.10.6` |
| Airflow | Lên lịch, chạy lại khi lỗi, nạp lịch sử theo tháng | `apache/airflow:3.3.2` (LocalExecutor) |

### Dữ liệu

[Credit Card Transactions Fraud Detection Dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection) — dữ liệu **mô phỏng** bằng công cụ Sparkov (dữ liệu thẻ thật của ngân hàng không bao giờ được công bố), giao dịch tại Mỹ, đơn vị USD.

| File | Số giao dịch | Thời gian | Dùng cho |
| --- | --- | --- | --- |
| `fraudTrain.csv` | 1.296.675 | 01/2019 → 06/2020 | Job batch `backfill` nạp lịch sử |
| `fraudTest.csv` | 555.719 | 06/2020 → 12/2020 | `producer` phát lại qua Kafka |

Dữ liệu thật không có lỗi hay bản trùng, nên producer **cài thêm** 0,5% bản ghi hỏng và 0,5% bản gửi trùng để kiểm tra DLQ và bỏ trùng. Giao dịch thật luôn được gửi đầy đủ.

### Bảo vệ dữ liệu cá nhân (PII)

| Cột ở Bronze (chỉ người có quyền xem) | Ở Silver trở thành |
| --- | --- |
| `card_number` | `card_token` = HMAC-SHA256(khóa bí mật, số thẻ) + `card_last4` |
| `dob` (ngày sinh) | `age` (tuổi lúc giao dịch) |
| `cust_lat`, `cust_long` (tọa độ nhà) | `distance_km` (khoảng cách nhà khách → cửa hàng) |
| `first_name`, `last_name`, `street`, `zip`, `job` | bỏ hẳn |

### Quyết định thiết kế

- **Ghim phiên bản mọi thứ**: lần chạy nào cũng giống nhau, không hỏng khi image `latest` thay đổi.
- **Job chạy như service** (`restart: unless-stopped`): chết thì Docker tự chạy lại, đọc tiếp đúng chỗ nhờ checkpoint.
- **Batch và stream dùng chung code**: mỗi dòng CSV được đổi thành đúng chuỗi JSON mà producer gửi vào Kafka, rồi đi qua cùng hàm `parse_events()` và cùng luật kiểm tra. `tests/test_backfill.py` chứng minh hai đường cho kết quả giống hệt.
- **Backfill chạy lại không trùng**: ghi bằng `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` theo `transaction_id`.
- **Silver đọc Kafka, không đọc bảng Bronze**: nguồn streaming Iceberg ghi file offset vào checkpoint bằng FileIO của bảng; Nessie luôn gửi `io-impl = S3FileIO` cho mỗi bảng (ghi đè cấu hình Spark), mà S3FileIO không ghi được checkpoint cục bộ → lỗi `Invalid S3 URI`. Hai job cùng đọc Kafka với checkpoint riêng; Bronze vẫn là bản lưu thô để đối chiếu và nạp lại.
- **Watermark bỏ trùng 2 giờ (theo thời gian giao dịch)**: producer tua nhanh 3600 lần, các partition Kafka có thể lệch nhau vài phút thời gian giao dịch; watermark ngắn sẽ làm rơi bản ghi đến trễ.
- **Partition theo tháng, không theo ngày**: dataset chỉ ~3 nghìn giao dịch/ngày; chia theo ngày sinh hàng nghìn file nhỏ và bắt mỗi mẻ stream mở hàng chục file cùng lúc. Bảng cũ được đổi tại chỗ bằng partition evolution của Iceberg.
- **Tắt `spark.sql.constraintPropagation`**: bộ lọc `error_reason IS NULL` (CASE WHEN của 10 luật) trên 29 cột làm bộ tối ưu Spark sinh ràng buộc theo cấp số nhân → tràn heap ngay lúc lập kế hoạch. `tests/test_streaming_plan.py` giữ lỗi này không quay lại.
- **Chấm điểm bằng scorecard WoE, không dùng mô hình hộp đen**: cách ngân hàng vẫn dùng cho chấm điểm rủi ro — mỗi cảnh báo có lý do đọc được (`hour=23`, `category_amount=shopping_net|800-1000`), bảng điểm nằm ngay trong lakehouse để kiểm toán.
- **Đánh giá trung thực**: học WoE và chọn ngưỡng chỉ trên TRAIN (fraudTrain); precision/recall báo cáo trên TEST (fraudTest). Đặc trưng theo thẻ chỉ dùng giao dịch **trước** giao dịch đang xét (`tests/test_gold.py` kiểm tra).
- **Airflow chỉ điều phối, không chạy việc nặng**: mỗi task là một container Spark/dbt riêng (`DockerOperator`, dùng đúng image của compose), xong thì tự xóa. Airflow 3 chạy dạng `standalone` trong một container để vừa máy 16 GB; môi trường production sẽ tách api-server, scheduler, dag-processor và chạy task trên Kubernetes.
- **Nạp lịch sử theo tháng bằng dynamic task mapping**: 18 task, tháng nào lỗi chạy lại tháng đó; job dùng MERGE nên chạy lại không trùng.
- **Test đối soát trong dbt**: Bronze (đếm theo `transaction_id`) = Silver = Gold, tổng tiền khớp tới từng xu, bảng cảnh báo khớp bảng đặc trưng; thêm **cổng chất lượng mô hình** (precision/recall trên TEST ≥ 0,5) — sửa code làm mô hình tệ đi thì pipeline báo đỏ.
- **Bảo trì Iceberg không dùng `expire_snapshots`/`remove_orphan_files`**: với Nessie, dọn snapshot và file thừa là việc của Nessie GC (biết file nào còn được branch/tag khác dùng). Job chỉ gộp file (`rewrite_data_files`) và manifest (`rewrite_manifests`).

---

## Nâng cấp từ Giai đoạn 1 (bạn đang có bản cũ chạy được)

Schema bảng đổi hoàn toàn, nên xóa dữ liệu cũ và chạy lại:

```bash
cd ~/realtime-banking-lakehouse4
unzip -o ~/Downloads/realtime-banking-lakehouse.zip -d .        # đè code mới, GIỮ file .env
cd realtime-banking-lakehouse
ls datasets/sparkov                                               # phải thấy fraudTrain.csv, fraudTest.csv
docker compose --profile realtime --profile jobs down -v --remove-orphans
docker compose --profile realtime --profile jobs build producer  # chỉ producer cần build lại
docker compose up -d
```

Sau đó làm tiếp từ **Bước 3** bên dưới.

---

## Cài đặt từ đầu

### Bước 0 — Chuẩn bị máy (làm 1 lần)

```bash
free -h      # dòng Mem, cột total: 16Gi là đủ
df -h ~      # cột Avail: cần trống ít nhất 30G
```

Cài Docker Engine (bỏ qua nếu `docker compose version` đã in ra phiên bản):

```bash
sudo apt remove -y docker.io docker-compose docker-doc podman-docker containerd runc
sudo apt update && sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER      # sau đó ĐĂNG XUẤT rồi đăng nhập lại
sudo apt install -y git unzip openssl
```

**Kết quả đúng:** `docker run --rm hello-world` chạy không cần `sudo`.

### Bước 1 — Code, khóa bí mật, dataset

```bash
mkdir -p ~/realtime-banking-lakehouse4 && cd ~/realtime-banking-lakehouse4
unzip -o ~/Downloads/realtime-banking-lakehouse.zip -d .
cd realtime-banking-lakehouse

# Khóa bí mật để băm số thẻ — chỉ tạo MỘT lần, đừng xóa (đổi khóa là mọi card_token đổi theo)
[ -f .env ] || echo "PII_HMAC_KEY=$(openssl rand -hex 32)" > .env

# Dataset: tải từ https://www.kaggle.com/datasets/kartik2112/fraud-detection (nút Download)
mkdir -p datasets/sparkov
unzip -o ~/Downloads/archive.zip -d datasets/sparkov
ls -lh datasets/sparkov
```

**Kết quả đúng:** `.env` có dòng `PII_HMAC_KEY=` + 64 ký tự; `datasets/sparkov` có `fraudTrain.csv` (335M) và `fraudTest.csv` (144M). Cả `.env` và `datasets/` đều nằm trong `.gitignore`.

### Bước 2 — Build image và bật hạ tầng

```bash
docker compose --profile realtime --profile jobs build    # lần đầu 5–15 phút
docker compose up -d
docker compose ps -a
```

**Kết quả đúng:** `object-store`, `nessie-db`, `nessie`, `kafka` **healthy**, `trino` **Up**, `create-bucket` **Exited (0)**.

---

## Bước 3 — Chạy unit test

```bash
docker compose --profile jobs run --rm spark-tools
```

**Kết quả đúng:** dòng cuối `28 passed, 1 skipped` (test DAG Airflow chỉ chạy ở nơi có cài Airflow, như CI).

| File test | Kiểm tra gì |
| --- | --- |
| `test_pii.py` | HMAC của Spark khớp thư viện `hmac` chuẩn; tuổi; khoảng cách haversine khớp công thức Python |
| `test_validation.py` | Từng luật nghiệp vụ bắt đúng loại lỗi; JSON hỏng không làm sập job |
| `test_pipeline.py` | Cả luồng streaming: mọi lỗi cài sẵn vào DLQ, Silver đủ giao dịch thật và hết bản trùng, không còn cột PII |
| `test_backfill.py` | Đọc CSV (batch) cho kết quả **giống hệt** đường Kafka (stream); lọc theo tháng đúng |
| `test_schema.py` | Cột ghi ra khớp đúng thứ tự và kiểu với `CREATE TABLE`, cho cả batch lẫn stream |

Test dùng dữ liệu mẫu tự sinh đúng định dạng Sparkov (`tests/sample_data.py`), không cần tải dataset.

---

## Bước 4 — Nạp lịch sử (batch backfill)

Thử nhanh 10.000 dòng trước để chắc mọi thứ ổn:

```bash
docker compose --profile jobs run --rm backfill --limit 10000
```

Rồi nạp cả file (khoảng 1,3 triệu dòng, mất 5–15 phút):

```bash
docker compose --profile jobs run --rm backfill
```

**Kết quả đúng** (ví dụ khi nạp cả file sau lần thử 10.000 dòng; dòng "vi pham luat" thường là 0 vì dữ liệu thật sạch):

```
[backfill] 1,296,675 dong, trong do 0 dong vi pham luat
[backfill] lakehouse.bronze.transactions_raw: +1,286,675 dong (tong 1,296,675)
[backfill] lakehouse.bronze.dlq: +0 dong (tong 0)
[backfill] lakehouse.silver.transactions: +1,286,675 dong (tong 1,296,675)
```

(Lần chạy đầy đủ chỉ thêm phần chưa có từ lần thử 10.000 dòng — đó là MERGE đang làm việc.) **Chạy lại lệnh lần nữa:** mọi dòng `+0` — chứng minh backfill không nhân đôi dữ liệu.

Nạp từng tháng (Giai đoạn 4, Airflow sẽ gọi đúng lệnh này): `docker compose --profile jobs run --rm backfill --month 2019-03`.

> Máy chậm hoặc báo thiếu RAM (`Exited (137)`): tạm dừng luồng realtime trước khi chạy backfill — `docker stop producer bronze-stream silver-stream`.

---

## Bước 5 — Bật luồng realtime

```bash
docker compose --profile realtime up -d
```

Thêm 3 container: `producer`, `bronze-stream`, `silver-stream`. Theo dõi log (`Ctrl + C` để thoát xem; dùng `docker logs` vì 3 container này thuộc profile `realtime`):

```bash
docker logs -f producer
docker logs -f bronze-stream
docker logs -f silver-stream
```

**Kết quả đúng** sau 1–2 phút:

```
producer       | [replay] da gui 3,412 dong (3,447 tin phien nay) | loi 0 | thoi gian giao dich: 2020-06-22T09:41:07Z
bronze-stream  | [bronze] bronze_valid me #3: doc 3,524 ban ghi tu Kafka
silver-stream  | [silver] silver       me #3: doc 3,524 ban ghi tu Kafka
```

Với `SPEED=3600`, cả fraudTest (≈ 6 tháng dữ liệu) phát xong trong khoảng 75–90 phút, rồi producer đứng yên (khi đó `kiem_tra.sh` sẽ cảnh báo "dữ liệu không còn chảy" — đúng như vậy). Producer nhớ vị trí đã gửi: tắt/bật lại sẽ gửi tiếp. Muốn phát lại từ đầu: đổi `RESET: "1"` trong `docker-compose.yml`, `docker compose --profile realtime up -d producer`, rồi đổi lại `"0"`.

Spark UI: http://localhost:4040 (Bronze), http://localhost:4041 (Silver), tab **Structured Streaming**.

---

## Bước 6 — Kiểm tra toàn bộ hệ thống bằng một lệnh

```bash
bash scripts/kiem_tra.sh
```

Script kiểm tra 9 nhóm — container, Kafka, log lỗi, dữ liệu có đang chảy không, chất lượng (DLQ, bỏ trùng), bảo mật PII, dữ liệu lịch sử, tầng Gold, dbt/bảo trì/Airflow — và in ✅ / ⚠️ / ❌ kèm gợi ý sửa. Dòng cuối `0 lỗi` là hệ thống chạy tốt. ⚠️ trong 3 phút đầu sau khi bật là bình thường.

---

## Bước 7 — Khám phá dữ liệu bằng Trino

```bash
docker exec -it trino trino
```

Chạy các câu trong [`sql/check_bronze.sql`](sql/check_bronze.sql) và [`sql/check_silver.sql`](sql/check_silver.sql). Đáng xem nhất:

| File · câu | Cho thấy |
| --- | --- |
| silver · 3 | Mỗi loại lỗi cài sẵn được đúng một luật bắt |
| silver · 4 | Bronze có bản trùng, Silver = 0 |
| silver · 6 | Silver không còn cột PII nào |
| silver · 8 | Dòng thời gian liền mạch: 2019-01 → 2020-06 từ batch, sau đó từ stream |
| silver · 9 | Danh mục có tỷ lệ gian lận cao nhất |
| silver · 10 | Độ trễ đầu-cuối p50/p95 từ lúc gửi Kafka tới Silver |

Xem file dữ liệu: http://localhost:9001 (admin / password) → bucket `warehouse`. Lịch sử commit của các bảng: http://localhost:19120.

---

## Bước 8 — Tầng Gold và chấm điểm gian lận

Chạy khi Silver đã có cả lịch sử (Bước 4) lẫn dữ liệu stream (Bước 5). Không cần tắt realtime; nếu máy chậm, `docker stop producer` trước.

```bash
docker compose --profile jobs run --rm gold
```

Job chạy vài phút, in ra:

1. Số giao dịch TRAIN / TEST và số vụ gian lận trong mỗi phần.
2. Sức mạnh (IV) của từng đặc trưng — đặc trưng nào thực sự giúp phát hiện gian lận.
3. Ngưỡng điểm chọn trên TRAIN, và **bảng đánh đổi precision ↔ recall trên TEST**.
4. Kết quả cuối trên TEST: precision, recall, F1, ROC-AUC.

Rồi xem chi tiết bằng [`sql/check_gold.sql`](sql/check_gold.sql): kết quả lần chạy mới nhất (câu 2), đặc trưng mạnh nhất (câu 4), hàng đợi cảnh báo kèm lý do (câu 6), gian lận bị bỏ sót (câu 7).

| Bảng | Chứa gì |
| --- | --- |
| `gold.transaction_features` | Mọi giao dịch: đặc trưng theo thẻ, điểm, có cảnh báo không, lý do |
| `gold.scorecard` | Bảng điểm: (đặc trưng, ô) → số giao dịch, tỷ lệ gian lận, WoE, IV |
| `gold.fraud_alerts` | Các giao dịch bị cảnh báo — hàng đợi cho người duyệt |
| `gold.model_metrics` | Precision/recall/F1/AUC mỗi lần chạy (giữ lịch sử để so sánh) |

Chạy lại bao nhiêu lần cũng được: 3 bảng đầu được thay nguyên khối trong một commit Iceberg, `model_metrics` thêm dòng mới.

---

## Bước 9 — dbt, bảo trì Iceberg và Airflow

Nên tắt luồng realtime (producer đã phát hết file) và Airflow của dự án khác để đủ RAM:

```bash
docker compose --profile realtime stop
printf '\nDOCKER_GID=%s\n' "$(getent group docker | cut -d: -f3)" >> .env   # làm 1 lần
docker compose --profile jobs --profile airflow build dbt airflow     # làm 1 lần
```

**Chạy tay từng job:**

```bash
docker compose --profile jobs run --rm maintenance   # gộp file nhỏ, in số file + thời gian truy vấn trước/sau
docker compose --profile jobs run --rm dbt           # dbt build: 3 bảng báo cáo + 18 test (gồm test đối soát)
```

**Hoặc để Airflow điều phối:**

```bash
docker compose --profile airflow up -d               # đợi ~1 phút, mở http://localhost:8089
```

| DAG | Lịch | Làm gì |
| --- | --- | --- |
| `lakehouse_daily` | 02:00 UTC hằng ngày | `gold_scoring` → `dbt_build` |
| `lakehouse_backfill` | chạy tay | 18 task, mỗi task nạp một tháng của fraudTrain (chạy lại không trùng) |
| `lakehouse_maintenance` | 03:00 UTC Chủ nhật | gộp file nhỏ mọi bảng, ghi `ops.maintenance_log` |

**Đã có sẵn Airflow trên máy** (2.4+ hoặc 3.x, chạy bằng docker compose)? Không cần bật Airflow riêng, chỉ cần:

```bash
bash scripts/noi_airflow_co_san.sh
```

Script tìm Airflow đang chạy, in ra phiên bản và việc sẽ làm, hỏi xác nhận, rồi thêm `docker-compose.override.yml` vào thư mục dự án Airflow đó (không sửa file nào có sẵn): gắn `airflow/dags` vào `dags/lakehouse`, cho quyền dùng Docker, đặt `HOST_PROJECT_DIR` và đọc `PII_HMAC_KEY` từ `.env` của dự án này. Gỡ: xóa file override rồi `docker compose up -d` trong thư mục Airflow. Toàn bộ DAG nằm trong **một file** `airflow/dags/lakehouse_dags.py` chạy được trên cả Airflow 2 và 3.

DAG mới tạo ở trạng thái tạm dừng: bật công tắc bên trái tên DAG, rồi bấm ▶ (Trigger) để chạy ngay. Xem kết quả bằng [`sql/check_marts.sql`](sql/check_marts.sql): tiền gian lận chặn được (câu 2), số file và thời gian truy vấn trước/sau bảo trì (câu 6).

---

## Bước 10 — Đẩy lên GitHub

```bash
git add .
git status            # KHÔNG được thấy .env hay datasets/
git commit -m "Giai doan 4: dbt + test doi soat, Airflow 3, bao tri Iceberg"
git push
```

Mỗi lần push, GitHub Actions tự chạy lint, unit test và kiểm tra `docker-compose.yml` (tab **Actions**).

---

## Vận hành hằng ngày

| Muốn | Lệnh |
| --- | --- |
| Bật toàn bộ từ đầu, tự kiểm tra từng bước | `bash scripts/start.sh` |
| Kiểm tra mọi thứ | `bash scripts/kiem_tra.sh` |
| Xem nhanh số bản ghi | `bash scripts/status.sh` |
| Xóa sạch kể cả đồ của phiên bản cũ (hỏi xác nhận) | `bash scripts/reset.sh` |
| Tạm dừng tất cả (giữ dữ liệu) | `docker compose --profile realtime stop` |
| Chạy lại | `docker compose --profile realtime start` |
| Tắt hẳn, giữ dữ liệu | `docker compose --profile realtime down` |
| Dừng producer cho máy nhẹ | `docker stop producer` |
| Sửa code Spark xong, chạy lại job | `docker restart bronze-stream silver-stream` |
| Sửa `replay.py` hoặc Dockerfile | `docker compose --profile realtime --profile jobs build` rồi `docker compose --profile realtime up -d` |
| Tính lại tầng Gold và điểm gian lận | `docker compose --profile jobs run --rm gold` |
| Dựng lại bảng báo cáo + chạy test đối soát | `docker compose --profile jobs run --rm dbt` |
| Gộp file nhỏ (bảo trì Iceberg) | `docker compose --profile jobs run --rm maintenance` |
| Bật / tắt Airflow | `docker compose --profile airflow up -d` / `docker compose --profile airflow stop` |
| Xem DAG có lỗi nạp không | `docker exec airflow airflow dags list-import-errors` |
| Gỡ một mẻ streaming bị kẹt (tràn bộ nhớ lặp lại) | `bash scripts/go_me_ket.sh` |
| Mở Spark SQL để thử | `docker compose --profile jobs run --rm spark-tools spark-sql` |
| **Xóa sạch, làm lại từ đầu** | `docker compose --profile realtime --profile jobs down -v` |

Code Spark và `spark-defaults.conf` được gắn thẳng từ máy vào container: sửa xong chỉ cần restart, không cần build lại.

---

## Xử lý lỗi thường gặp

| Hiện tượng | Cách xử lý |
| --- | --- |
| `permission denied ... docker.sock` | `sudo usermod -aG docker $USER` rồi đăng xuất / đăng nhập lại |
| `Cannot connect to the Docker daemon` | `sudo systemctl start docker` |
| `silver-stream` hoặc `backfill` báo `Thieu PII_HMAC_KEY` | Thư mục hiện tại chưa có `.env` (thường do giải nén ra thư mục mới). Làm lại Bước 1, rồi `docker compose --profile realtime up -d --force-recreate silver-stream` |
| `producer` báo `Khong thay file /data/sparkov/fraudTest.csv` | Chưa giải nén dataset vào `datasets/sparkov/` (Bước 1) |
| `silver-stream` báo `Invalid S3 URI ... file:/opt/spark/checkpoints` | Đang chạy code cũ (Silver đọc bảng Bronze). Giải nén bản mới rồi `docker compose --profile realtime up -d --force-recreate silver-stream` |
| `No such service` khi `docker compose logs ...` | Service thuộc profile `realtime`: dùng `docker logs -f <tên>` |
| Build lỗi ở `ADD https://repo1.maven.org/...` | Mạng chập chờn khi tải thư viện Java. Chạy lại lệnh build |
| Container `Exited (137)` | Thiếu RAM: `docker stop producer`, đóng bớt ứng dụng, không chạy backfill cùng lúc với realtime |
| Job streaming tràn heap lặp đi lặp lại, stack trace có `catalyst ... canonicalized` | Thiếu dòng `spark.sql.constraintPropagation.enabled false` trong `docker/spark/spark-defaults.conf`. Thêm vào rồi `docker compose --profile realtime up -d --force-recreate bronze-stream silver-stream` |
| Job streaming tràn heap mãi ở cùng một mẻ quá lớn | `bash scripts/go_me_ket.sh` (xóa kế hoạch của mẻ chưa hoàn thành; không mất, không trùng dữ liệu) |
| Trino báo `server is still initializing` giữa chừng, `kiem_tra.sh` ra dấu `?` | Trino vừa bị tắt vì vượt RAM. Kiểm tra có file `docker/trino/jvm.config` (heap 2 GB) và `mem_limit: 3g`, rồi `docker compose up -d --force-recreate trino` |
| `gold` báo `TRAIN khong co giao dich gian lan nao` | Chưa nạp lịch sử: chạy backfill (Bước 4) trước |
| `noi_airflow_co_san.sh` báo `docker-compose.override.yml da ton tai` | Dự án Airflow đã có file override của riêng nó: thêm tay các mục trong script (volumes, group_add, environment, env_file) vào file đó |
| Airflow báo lỗi nạp DAG `HOST_PROJECT_DIR chua dat` | Airflow riêng: đã bật từ thư mục khác, `cd` vào thư mục dự án rồi `docker compose --profile airflow up -d --force-recreate airflow`. Airflow có sẵn: chạy lại `bash scripts/noi_airflow_co_san.sh` |
| Task Airflow lỗi `Permission denied ... docker.sock` | Thiếu hoặc sai `DOCKER_GID` trong `.env`: `getent group docker \| cut -d: -f3`, sửa `.env`, rồi `--force-recreate airflow` |
| Task Airflow lỗi `No such image: lakehouse-dbt...` | Chưa build image dbt: `docker compose --profile jobs build dbt` |
| dbt: `assert_bronze_silver_reconcile` hoặc `assert_silver_gold_reconcile` thất bại | Luồng streaming chưa đuổi kịp, hoặc Silver có dữ liệu mới mà job gold chưa chạy lại. Đợi stream xong / chạy `gold`, rồi chạy lại dbt |
| `port is already allocated` | `sudo ss -ltnp \| grep <cổng>`; đổi số **bên trái** trong `ports:` |

Gặp lỗi khác: chạy `bash scripts/kiem_tra.sh` và gửi toàn bộ kết quả.

---

## Cấu trúc thư mục

```
realtime-banking-lakehouse/
├── docker-compose.yml              # Toàn bộ hệ thống; profile: (mặc định) / realtime / jobs
├── .env.example                    # Mẫu .env (khóa HMAC) — .env thật không đưa lên GitHub
├── .github/workflows/ci.yml        # CI: lint, unit test, kiểm tra compose
├── datasets/sparkov/               # Dataset tải từ Kaggle (không đưa lên GitHub)
├── docker/
│   ├── spark/                      # Dockerfile Spark 3.5.9 + Iceberg + Kafka; spark-defaults.conf
│   ├── trino/                      # catalog/iceberg.properties, jvm.config (heap 2 GB)
│   ├── dbt/Dockerfile              # Image dbt-core + dbt-trino
│   └── airflow/Dockerfile          # Airflow 3.3.2 + provider Docker
├── dbt/                            # Dự án dbt: models/marts/*.sql, tests/ (đối soát, cổng chất lượng)
├── airflow/dags/lakehouse_dags.py   # 3 DAG trong 1 file (Airflow 2.4+ và 3.x)
├── data_generator/replay.py        # Producer phát lại CSV vào Kafka theo nhịp thời gian
├── spark_jobs/
│   ├── bronze_ingestion.py         # Stream: Kafka -> Bronze + DLQ
│   ├── silver_transformation.py    # Stream: Kafka -> Silver
│   ├── batch_backfill.py           # Batch: CSV lịch sử -> Bronze + DLQ + Silver (MERGE)
│   ├── gold_scoring.py             # Batch: Silver -> đặc trưng, scorecard, cảnh báo, đánh giá
│   ├── iceberg_maintenance.py      # Batch: gộp file nhỏ, đo trước/sau -> ops.maintenance_log
│   └── common/
│       ├── schema.py               # Sự kiện chuẩn: CSV/Kafka -> JSON -> cột có kiểu
│       ├── validation.py           # 10 luật nghiệp vụ
│       ├── pii.py                  # HMAC, tuổi, khoảng cách
│       ├── tables.py               # DDL các bảng, bỏ trùng, biến đổi sang Silver
│       ├── gold.py                 # Đặc trưng theo thẻ, WoE, chấm điểm, precision/recall/AUC
│       └── streaming.py            # Đọc Kafka, in tiến độ
├── tests/                          # 29 unit test (1 cần Airflow) + dữ liệu mẫu tự sinh
├── sql/                            # check_bronze / check_silver / check_gold / check_marts cho Trino
└── scripts/
    ├── kiem_tra.sh                 # Kiểm tra sức khỏe toàn hệ thống bằng 1 lệnh
    ├── noi_airflow_co_san.sh       # Nối dự án vào Airflow có sẵn trên máy
    └── go_me_ket.sh                # Gỡ mẻ streaming bị kẹt
```

## Bản đồ cổng

| Địa chỉ | Là gì |
| --- | --- |
| http://localhost:19120 | Nessie UI — branch, commit, danh sách bảng |
| http://localhost:9001 | Kho S3 (RustFS) — admin / password |
| http://localhost:8085 | Trino UI |
| http://localhost:8089 | Airflow (không cần đăng nhập — chỉ dùng trên máy cá nhân) |
| http://localhost:4040 / 4041 / 4042 / 4043 / 4044 | Spark UI của Bronze / Silver / backfill / gold / maintenance |
| localhost:9092 | Kafka (cho chương trình chạy trên máy bạn) |

## Lộ trình

- [x] **Giai đoạn 1** — Nền móng: Kafka → Bronze + DLQ → Silver, HMAC, ghim phiên bản, Nessie, job như service, CI
- [x] **Giai đoạn 2** — Dữ liệu thật Sparkov: producer phát lại, batch backfill dùng chung code với stream, PII mở rộng
- [x] **Giai đoạn 3** — Gold: đặc trưng theo thẻ, scorecard WoE có lý do; TEST: precision 0,62 · recall 0,62 · F1 0,62
- [ ] **Giai đoạn 4** — dbt (mart + test đối soát + cổng chất lượng), Airflow 3 (gold → dbt hằng ngày, backfill theo tháng), bảo trì Iceberg đo trước/sau *(đang làm)*
- [ ] **Giai đoạn 5** — Superset dashboard dạng code; đo độ trễ p50/p95
- [ ] **Giai đoạn 6** — CD (đẩy image lên GHCR), benchmark trước/sau bảo trì, hoàn thiện README

> Mật khẩu `admin / password` chỉ dùng cho môi trường học tập trên máy cá nhân.
