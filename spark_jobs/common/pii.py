"""
Bảo vệ dữ liệu cá nhân (PII) ở tầng Silver.

  card_number -> card_token = HMAC-SHA256(secret_key, card_number)
                 + Không giải ngược được, không dò ngược bằng brute-force được vì
                   kẻ tấn công không có secret_key (khác SHA-256 thường).
                 + Cùng một số thẻ luôn ra cùng một token -> vẫn đếm/nhóm theo thẻ được.
              -> card_last4 = 4 số cuối, để hiển thị.
  dob         -> age (số tuổi lúc giao dịch). Ngày sinh đầy đủ bị bỏ.
  cust_lat/long + merch_lat/long -> distance_km (khoảng cách nhà khách - cửa hàng).
                 Tọa độ nhà khách bị bỏ.
  họ tên, địa chỉ, zip, nghề nghiệp -> bỏ hẳn.

HMAC dựng bằng hàm sha2() có sẵn của Spark theo đúng RFC 2104:
    HMAC(K, m) = H((K xor opad) || H((K xor ipad) || m))
nên chạy hoàn toàn trong JVM. tests/test_pii.py đối chiếu với thư viện hmac của Python.
"""

import hashlib
import os

from pyspark.sql import Column
from pyspark.sql import functions as F

_BLOCK_SIZE = 64  # kích thước khối của SHA-256 (byte)
EARTH_RADIUS_KM = 6371.0


def load_key() -> bytes:
    """Đọc secret key từ biến môi trường PII_HMAC_KEY (đặt trong file .env)."""
    key = os.getenv("PII_HMAC_KEY", "")
    if len(key) < 32:
        raise SystemExit(
            "[pii] Thieu PII_HMAC_KEY hoac qua ngan (can >= 32 ky tu). "
            "Tao file .env trong thu muc du an (xem README, Buoc 1) roi chay lai "
            "'docker compose --profile realtime up -d --force-recreate'.")
    return key.encode("utf-8")


def _pads(key: bytes) -> tuple:
    if len(key) > _BLOCK_SIZE:
        key = hashlib.sha256(key).digest()
    key = key.ljust(_BLOCK_SIZE, b"\x00")
    ipad = bytes(b ^ 0x36 for b in key)
    opad = bytes(b ^ 0x5C for b in key)
    return ipad, opad


def hmac_sha256(col: Column, key: bytes) -> Column:
    """HMAC-SHA256 dạng chuỗi hex 64 ký tự. NULL vào -> NULL ra."""
    ipad, opad = _pads(key)
    msg = F.encode(col, "UTF-8")
    inner = F.unhex(F.sha2(F.concat(F.lit(bytearray(ipad)), msg), 256))
    outer = F.sha2(F.concat(F.lit(bytearray(opad)), inner), 256)
    return F.when(col.isNull(), F.lit(None)).otherwise(outer)


def last4(col: Column) -> Column:
    return F.substring(col, -4, 4)


def age_years(dob: Column, at: Column) -> Column:
    """Số tuổi tròn tại thời điểm `at`."""
    return F.floor(F.months_between(F.to_date(at), dob) / 12).cast("int")


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Khoảng cách theo đường tròn lớn giữa hai tọa độ (km)."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))
