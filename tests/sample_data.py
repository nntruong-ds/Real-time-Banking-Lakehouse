"""
Sinh dữ liệu MẪU đúng định dạng file Sparkov (fraudTrain.csv / fraudTest.csv) để test.

Không dùng dữ liệu thật trong test: test chạy được ở mọi máy (kể cả CI trên GitHub)
mà không cần tải dataset. Các giá trị là ngẫu nhiên, chỉ giữ đúng tên cột, kiểu dữ liệu
và vài "cái bẫy" của file thật: tên merchant có dấu phẩy, tiền tố "fraud_", cột đầu
không có tên.
"""

import csv
import random
from datetime import datetime, timedelta

HEADER = ["", "trans_date_trans_time", "cc_num", "merchant", "category", "amt", "first",
          "last", "gender", "street", "city", "state", "zip", "lat", "long", "city_pop",
          "job", "dob", "trans_num", "unix_time", "merch_lat", "merch_long", "is_fraud"]

MERCHANTS = ["fraud_Rippin, Kub and Mann", "fraud_Heller, Gutmann and Zieme",
             "fraud_Lind-Buckridge", "fraud_Kutch, Hermiston and Farrell", "fraud_Keeling-Crist"]
CATEGORIES = ["misc_net", "grocery_pos", "entertainment", "gas_transport", "shopping_net"]
JOBS = ["Psychologist, counselling", "Special educational needs teacher",
        "Nature conservation officer"]


def make_rows(n: int = 300, seed: int = 7, start: str = "2020-06-21 12:00:00") -> list:
    rng = random.Random(seed)
    cards = [{"cc_num": str(rng.randrange(10**15, 10**16)),
              "first": f"First{i}", "last": f"Last{i}", "gender": rng.choice("FM"),
              "street": f"{rng.randint(1, 999)} Example St", "city": f"City{i % 7}",
              "state": rng.choice(["NC", "WA", "TX", "CA"]),
              "zip": str(rng.randint(10000, 99999)),
              "lat": round(rng.uniform(25, 48), 4), "long": round(rng.uniform(-123, -70), 4),
              "city_pop": rng.randint(100, 2_000_000), "job": rng.choice(JOBS),
              "dob": f"{rng.randint(1940, 2002)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"}
             for i in range(25)]
    t = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    rows = []
    for i in range(n):
        t += timedelta(seconds=rng.randint(1, 120))
        c = rng.choice(cards)
        rows.append({
            "": str(i),
            "trans_date_trans_time": t.strftime("%Y-%m-%d %H:%M:%S"),
            "cc_num": c["cc_num"],
            "merchant": rng.choice(MERCHANTS),
            "category": rng.choice(CATEGORIES),
            "amt": f"{rng.uniform(1, 900):.2f}",
            "first": c["first"], "last": c["last"], "gender": c["gender"],
            "street": c["street"], "city": c["city"], "state": c["state"], "zip": c["zip"],
            "lat": str(c["lat"]), "long": str(c["long"]), "city_pop": str(c["city_pop"]),
            "job": c["job"], "dob": c["dob"],
            "trans_num": f"{rng.getrandbits(128):032x}",
            "unix_time": str(int(t.timestamp())),
            "merch_lat": str(round(c["lat"] + rng.uniform(-1, 1), 6)),
            "merch_long": str(round(c["long"] + rng.uniform(-1, 1), 6)),
            "is_fraud": "1" if rng.random() < 0.05 else "0",
        })
    return rows


def write_csv(rows: list, path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
