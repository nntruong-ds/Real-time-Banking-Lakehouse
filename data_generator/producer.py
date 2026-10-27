"""
Producer — sinh giao dịch thẻ ngân hàng giả lập và gửi liên tục vào Kafka.

Mỗi giao dịch là một bản ghi JSON, ví dụ:
{
  "transaction_id": "5f0c...",
  "card_number": "4111111111111111",
  "account_id": "ACC000123",
  "phone": "0912345678",
  "amount": 250000.0,
  "currency": "VND",
  "merchant_id": "MER0042",
  "city": "Ha Noi",
  "event_time": "2026-10-23T03:15:02.123Z",
  "sim_scenario": "normal"
}

Ba loại bản ghi được sinh ra:
  - normal   : giao dịch bình thường (phần lớn)
  - burst/geo: kịch bản gian lận, dùng để kiểm tra Mốc 3
               burst = một thẻ quẹt dồn dập nhiều lần trong vài chục giây
               geo   = một thẻ quẹt ở 2 thành phố xa nhau trong vài phút
  - invalid  : bản ghi sai nghiệp vụ, dùng để kiểm tra DLQ ở Mốc 2
  - bản ghi trùng: thỉnh thoảng một giao dịch bị gửi 2 lần (cùng
               transaction_id), giống lỗi retry ngoài đời; Mốc 2 phải bỏ trùng

Cột "sim_scenario" chỉ có trong dữ liệu giả lập, giúp bạn kiểm tra kết quả.
Dữ liệu ngân hàng thật không có cột này.

Cấu hình qua biến môi trường (xem docker-compose.yml):
  KAFKA_BOOTSTRAP, KAFKA_TOPIC, RATE, INVALID_RATIO, FRAUD_RATIO,
  DUPLICATE_RATIO
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker

# ---------------------------------------------------------------------------
# Cấu hình
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
RATE = float(os.getenv("RATE", "50"))                  # giao dịch / giây
INVALID_RATIO = float(os.getenv("INVALID_RATIO", "0.005"))
FRAUD_RATIO = float(os.getenv("FRAUD_RATIO", "0.003"))
DUPLICATE_RATIO = float(os.getenv("DUPLICATE_RATIO", "0.005"))
NUM_ACCOUNTS = int(os.getenv("NUM_ACCOUNTS", "2000"))
SEED = int(os.getenv("SEED", "42"))

CITIES = [
    "Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho",
    "Nha Trang", "Phu Quoc", "Hue", "Vung Tau", "Da Lat",
]
# Hai thành phố cách xa nhau, dùng cho kịch bản gian lận "geo"
FAR_CITY_PAIRS = [("Ha Noi", "Ho Chi Minh"), ("Hai Phong", "Can Tho"),
                  ("Hue", "Phu Quoc"), ("Ha Noi", "Phu Quoc")]
NUM_MERCHANTS = 500

fake = Faker("vi_VN")
Faker.seed(SEED)
random.seed(SEED)


# ---------------------------------------------------------------------------
# Tạo sẵn danh sách khách hàng: mỗi tài khoản có 1 thẻ, 1 SĐT, 1 thành phố
# quen thuộc và mức chi tiêu trung bình riêng.
# ---------------------------------------------------------------------------
def build_accounts(n):
    accounts = []
    for i in range(n):
        accounts.append({
            "account_id": f"ACC{i:06d}",
            "card_number": fake.credit_card_number(card_type="visa16"),
            "phone": "09" + "".join(random.choices("0123456789", k=8)),
            "home_city": random.choice(CITIES),
            # mức chi tiêu trung bình của mỗi người: 100 nghìn – 2 triệu VND
            "avg_amount": random.uniform(100_000, 2_000_000),
        })
    return accounts


def now_iso(offset_seconds=0.0):
    t = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_transaction(acc, city=None, amount=None, scenario="normal",
                     offset_seconds=0.0):
    if amount is None:
        # số tiền dao động quanh mức trung bình của chủ thẻ
        amount = max(10_000, random.gauss(acc["avg_amount"],
                                          acc["avg_amount"] * 0.4))
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_number": acc["card_number"],
        "account_id": acc["account_id"],
        "phone": acc["phone"],
        "amount": round(amount, -2),          # làm tròn tới hàng trăm đồng
        "currency": "VND",
        "merchant_id": f"MER{random.randint(1, NUM_MERCHANTS):04d}",
        "city": city or acc["home_city"],
        "event_time": now_iso(offset_seconds),
        "sim_scenario": scenario,
    }


def make_invalid(acc):
    """Bản ghi SAI NGHIỆP VỤ — Mốc 2 sẽ tách những bản ghi này sang DLQ."""
    txn = make_transaction(acc, scenario="invalid")
    kind = random.choice(["negative_amount", "unknown_currency",
                          "future_time", "missing_id", "zero_amount"])
    if kind == "negative_amount":
        txn["amount"] = -abs(txn["amount"])
    elif kind == "zero_amount":
        txn["amount"] = 0
    elif kind == "unknown_currency":
        txn["currency"] = "XXX"
    elif kind == "future_time":
        txn["event_time"] = now_iso(offset_seconds=3 * 24 * 3600)
    elif kind == "missing_id":
        txn["transaction_id"] = None
    txn["sim_invalid_reason"] = kind
    return [txn]


def make_fraud_burst(acc):
    """Kịch bản gian lận: 6–10 giao dịch liên tiếp, mỗi cái cách nhau vài giây."""
    n = random.randint(6, 10)
    return [make_transaction(acc, scenario="fraud_burst",
                             amount=random.uniform(2_000_000, 9_000_000),
                             offset_seconds=i * random.uniform(2, 6))
            for i in range(n)]


def make_fraud_geo(acc):
    """Kịch bản gian lận: cùng một thẻ, 2 thành phố xa nhau trong ~2 phút."""
    city_a, city_b = random.choice(FAR_CITY_PAIRS)
    return [
        make_transaction(acc, city=city_a, scenario="fraud_geo"),
        make_transaction(acc, city=city_b, scenario="fraud_geo",
                         offset_seconds=random.uniform(30, 120)),
    ]


def next_batch(accounts):
    """Chọn ngẫu nhiên loại bản ghi tiếp theo theo tỷ lệ cấu hình."""
    acc = random.choice(accounts)
    r = random.random()
    if r < INVALID_RATIO:
        return make_invalid(acc)
    if r < INVALID_RATIO + FRAUD_RATIO:
        return random.choice([make_fraud_burst, make_fraud_geo])(acc)
    if r < INVALID_RATIO + FRAUD_RATIO + DUPLICATE_RATIO:
        txn = make_transaction(acc)
        return [txn, dict(txn)]     # gửi 2 lần y hệt nhau
    return [make_transaction(acc)]


# ---------------------------------------------------------------------------
# Gửi vào Kafka
# ---------------------------------------------------------------------------
def create_producer():
    from confluent_kafka import Producer  # import ở đây để test không cần Kafka

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,     # Kafka tự chống gửi trùng khi retry
        "acks": "all",
        "linger.ms": 20,                # gom vài ms để gửi theo lô, nhanh hơn
        "compression.type": "lz4",
    }
    # Kafka có thể chưa sẵn sàng ngay -> thử lại vài lần
    for attempt in range(1, 31):
        try:
            p = Producer(conf)
            p.list_topics(timeout=5)
            print(f"[producer] Da ket noi Kafka tai {KAFKA_BOOTSTRAP}",
                  flush=True)
            return p
        except Exception as exc:  # noqa: BLE001
            print(f"[producer] Cho Kafka (lan {attempt}): {exc}", flush=True)
            time.sleep(5)
    sys.exit("[producer] Khong ket noi duoc Kafka")


def main():
    accounts = build_accounts(NUM_ACCOUNTS)
    producer = create_producer()

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent, failed = 0, 0
    counts = {}
    last_report = time.time()
    interval = 1.0 / RATE if RATE > 0 else 0

    def on_delivery(err, _msg):
        nonlocal failed
        if err is not None:
            failed += 1

    print(f"[producer] Bat dau gui vao topic '{KAFKA_TOPIC}' "
          f"voi toc do ~{RATE:g} giao dich/giay", flush=True)

    while running:
        started = time.time()
        for txn in next_batch(accounts):
            producer.produce(
                KAFKA_TOPIC,
                # key = account_id: giao dịch cùng tài khoản luôn vào cùng
                # partition, nên giữ đúng thứ tự
                key=txn["account_id"],
                value=json.dumps(txn, ensure_ascii=False),
                on_delivery=on_delivery,
            )
            sent += 1
            counts[txn["sim_scenario"]] = counts.get(txn["sim_scenario"], 0) + 1
        producer.poll(0)

        if time.time() - last_report >= 10:
            print(f"[producer] da gui {sent:,} ban ghi | loi {failed} | "
                  f"theo loai: {counts}", flush=True)
            last_report = time.time()

        sleep_for = interval - (time.time() - started)
        if sleep_for > 0:
            time.sleep(sleep_for)

    print("[producer] Dang dung, xa het ban ghi con lai...", flush=True)
    producer.flush(10)
    print(f"[producer] Da dung. Tong cong {sent:,} ban ghi.", flush=True)


if __name__ == "__main__":
    main()
