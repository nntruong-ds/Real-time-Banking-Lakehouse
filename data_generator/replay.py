"""
Producer phát lại (replay) dataset giao dịch thẻ Sparkov vào Kafka.

Giả lập hệ thống thanh toán đang chạy thật: đọc file CSV theo thứ tự thời gian và
gửi từng giao dịch vào Kafka theo đúng nhịp của cột thời gian, tua nhanh SPEED lần.
Ví dụ SPEED=3600: 1 giờ dữ liệu được phát trong 1 giây (1 ngày trong 24 giây).

Mỗi dòng CSV -> một sự kiện JSON "chuẩn" (canonical) mà mọi job Spark dùng chung:
    transaction_id, card_number, merchant, category, amount, currency,
    first_name, last_name, gender, street, city, state, zip, cust_lat, cust_long,
    city_pop, job, dob, merch_lat, merch_long, event_time, is_fraud, sim_scenario

Dữ liệu thật không có lỗi định dạng hay bản trùng, nên producer CÀI THÊM một ít
để kiểm tra DLQ và bỏ trùng (giao dịch thật luôn được gửi đầy đủ, không bị thay):
  - INVALID_RATIO   : gửi kèm một bản ghi hỏng (sim_scenario = "invalid")
  - DUPLICATE_RATIO : gửi giao dịch đó 2 lần (giống lỗi retry ngoài đời)

Chạy lại sau khi bị tắt: producer nhớ đã gửi tới dòng nào (file trong STATE_DIR)
và gửi tiếp từ đó. Đặt RESET=1 để phát lại từ đầu file.

Biến môi trường: KAFKA_BOOTSTRAP, KAFKA_TOPIC, DATA_FILE, SPEED, MAX_RATE,
INVALID_RATIO, DUPLICATE_RATIO, STATE_DIR, RESET, SEED
"""

import csv
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
DATA_FILE = os.getenv("DATA_FILE", "/data/sparkov/fraudTest.csv")
SPEED = float(os.getenv("SPEED", "3600"))          # tua nhanh bao nhiêu lần
MAX_RATE = float(os.getenv("MAX_RATE", "300"))     # tối đa giao dịch / giây
INVALID_RATIO = float(os.getenv("INVALID_RATIO", "0.005"))
DUPLICATE_RATIO = float(os.getenv("DUPLICATE_RATIO", "0.005"))
STATE_DIR = os.getenv("STATE_DIR", "/state")
RESET = os.getenv("RESET", "0") == "1"
SEED = int(os.getenv("SEED", "42"))

CSV_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
INVALID_KINDS = ["negative_amount", "zero_amount", "unknown_currency",
                 "future_time", "missing_id", "bad_coordinates"]


# ---------------------------------------------------------------------------
# Chuyển một dòng CSV Sparkov thành sự kiện chuẩn
# ---------------------------------------------------------------------------
def _num(value, cast):
    """Ép kiểu số; nếu hỏng thì giữ nguyên chuỗi để luật kiểm tra phía Spark bắt."""
    try:
        return cast(value)
    except (TypeError, ValueError):
        return value


def parse_csv_time(value: str) -> datetime:
    return datetime.strptime(value, CSV_TIME_FORMAT).replace(tzinfo=timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def row_to_event(row: dict) -> dict:
    """Một dòng CSV (dict theo tên cột gốc) -> sự kiện chuẩn. Giờ trong file coi là UTC."""
    try:
        event_time = to_iso(parse_csv_time(row["trans_date_trans_time"]))
    except (KeyError, ValueError):
        event_time = row.get("trans_date_trans_time")
    return {
        "transaction_id": row.get("trans_num") or None,
        "card_number": row.get("cc_num") or None,
        "merchant": row.get("merchant") or None,
        "category": row.get("category") or None,
        "amount": _num(row.get("amt"), float),
        "currency": "USD",               # dataset là giao dịch tại Mỹ
        "first_name": row.get("first") or None,
        "last_name": row.get("last") or None,
        "gender": row.get("gender") or None,
        "street": row.get("street") or None,
        "city": row.get("city") or None,
        "state": row.get("state") or None,
        "zip": row.get("zip") or None,
        "cust_lat": _num(row.get("lat"), float),
        "cust_long": _num(row.get("long"), float),
        "city_pop": _num(row.get("city_pop"), int),
        "job": row.get("job") or None,
        "dob": row.get("dob") or None,
        "merch_lat": _num(row.get("merch_lat"), float),
        "merch_long": _num(row.get("merch_long"), float),
        "event_time": event_time,
        "is_fraud": _num(row.get("is_fraud"), int),
        "sim_scenario": "real",
    }


def make_invalid(event: dict, rng: random.Random) -> dict:
    """Bản sao HỎNG của một giao dịch, mã giao dịch mới để không đụng bản thật."""
    bad = dict(event, transaction_id=f"inj-{uuid.UUID(int=rng.getrandbits(128))}",
               sim_scenario="invalid")
    kind = rng.choice(INVALID_KINDS)
    if kind == "negative_amount":
        bad["amount"] = -abs(float(bad["amount"] or 1))
    elif kind == "zero_amount":
        bad["amount"] = 0
    elif kind == "unknown_currency":
        bad["currency"] = "XXX"
    elif kind == "future_time":
        bad["event_time"] = to_iso(datetime.now(timezone.utc) + timedelta(days=3))
    elif kind == "missing_id":
        bad["transaction_id"] = None
    elif kind == "bad_coordinates":
        bad["merch_lat"] = 999.0
    bad["sim_invalid_reason"] = kind
    return bad


def with_faults(event: dict, rng: random.Random) -> list:
    """Giao dịch thật luôn được gửi; đôi khi kèm bản hỏng hoặc bản trùng."""
    r = rng.random()
    if r < INVALID_RATIO:
        return [event, make_invalid(event, rng)]
    if r < INVALID_RATIO + DUPLICATE_RATIO:
        return [event, dict(event)]
    return [event]


# ---------------------------------------------------------------------------
# Nhớ vị trí đã gửi
# ---------------------------------------------------------------------------
def state_path() -> str:
    return os.path.join(STATE_DIR, os.path.basename(DATA_FILE) + ".offset")


def load_offset() -> int:
    if RESET:
        return 0
    try:
        with open(state_path()) as f:
            return int(f.read().strip() or 0)
    except (FileNotFoundError, ValueError):
        return 0


def save_offset(n: int) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = state_path() + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(n))
    os.replace(tmp, state_path())


# ---------------------------------------------------------------------------
# Gửi vào Kafka
# ---------------------------------------------------------------------------
def create_producer():
    from confluent_kafka import Producer  # import ở đây để unit test không cần Kafka

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    }
    for attempt in range(1, 31):
        try:
            p = Producer(conf)
            p.list_topics(timeout=5)
            print(f"[replay] Da ket noi Kafka tai {KAFKA_BOOTSTRAP}", flush=True)
            return p
        except Exception as exc:  # noqa: BLE001
            print(f"[replay] Cho Kafka (lan {attempt}): {exc}", flush=True)
            time.sleep(5)
    sys.exit("[replay] Khong ket noi duoc Kafka")


def main():
    if not os.path.exists(DATA_FILE):
        sys.exit(f"[replay] Khong thay file {DATA_FILE}. Giai nen dataset vao "
                 "datasets/sparkov/ theo README (Giai doan 2).")

    rng = random.Random(SEED)
    producer = create_producer()
    skip = load_offset()

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    failed = 0

    def on_delivery(err, _msg):
        nonlocal failed
        if err is not None:
            failed += 1

    print(f"[replay] Phat lai {DATA_FILE} vao topic '{KAFKA_TOPIC}', toc do x{SPEED:g}, "
          f"bo qua {skip:,} dong da gui truoc do", flush=True)

    rows_done = skip
    msgs_sent = 0
    t0_event = t0_wall = None
    last_send = 0.0
    last_report = time.time()
    min_gap = 1.0 / MAX_RATE if MAX_RATE > 0 else 0.0
    event_time = "-"

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if i < skip:
                continue
            if not running:
                break

            event = row_to_event(row)
            event_time = event["event_time"]

            # Giữ nhịp theo thời gian giao dịch, tua nhanh SPEED lần
            try:
                t_event = parse_csv_time(row["trans_date_trans_time"])
            except (KeyError, ValueError):
                t_event = None
            if t_event is not None and SPEED > 0:
                if t0_event is None:
                    t0_event, t0_wall = t_event, time.time()
                due = t0_wall + (t_event - t0_event).total_seconds() / SPEED
                wait = max(due, last_send + min_gap) - time.time()
                if wait > 0:
                    time.sleep(min(wait, 5.0))

            for msg in with_faults(event, rng):
                producer.produce(
                    KAFKA_TOPIC,
                    key=str(msg.get("card_number") or ""),  # cùng thẻ -> cùng partition
                    value=json.dumps(msg, ensure_ascii=False),
                    on_delivery=on_delivery,
                )
                msgs_sent += 1
            producer.poll(0)
            last_send = time.time()
            rows_done = i + 1

            if time.time() - last_report >= 10:
                producer.flush(10)
                save_offset(rows_done)
                print(f"[replay] da gui {rows_done:,} dong ({msgs_sent:,} tin phien nay) | "
                      f"loi {failed} | thoi gian giao dich: {event_time}", flush=True)
                last_report = time.time()

    producer.flush(30)
    save_offset(rows_done)
    if running:
        print(f"[replay] Da phat het file: {rows_done:,} dong. Dat RESET=1 de phat lai "
              "tu dau. Producer dung o day.", flush=True)
        while running:          # đứng yên thay vì thoát, để Docker không bật lại liên tục
            time.sleep(5)
    print(f"[replay] Da dung o dong {rows_done:,} (thoi gian giao dich {event_time}).",
          flush=True)


if __name__ == "__main__":
    main()
