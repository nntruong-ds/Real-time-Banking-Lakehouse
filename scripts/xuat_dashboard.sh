#!/usr/bin/env bash
# =============================================================================
# Lưu dashboard Superset NGƯỢC VỀ CODE (thư mục superset/ của repo).
#
#     bash scripts/xuat_dashboard.sh
#
# Dùng khi bạn vừa sửa dashboard trên giao diện (thêm chart, đổi bố cục...). Sau đó
# "git diff superset/" cho thấy đúng những gì đã đổi, dạng YAML, và commit như code bình thường.
# Lần khởi động sau, Superset nạp lại đúng bản trong git.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1

docker inspect superset >/dev/null 2>&1 || { echo "LOI: container superset chua chay (docker compose --profile bi up -d)"; exit 1; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "-> Xuat dashboard tu Superset..."
docker exec superset superset export-dashboards -f /tmp/lakehouse_export.zip >/dev/null 2>&1
docker cp superset:/tmp/lakehouse_export.zip "$TMP/export.zip" >/dev/null
python3 - "$TMP/export.zip" superset <<'PY'
import os, re, shutil, sys, zipfile
src_zip, dest = sys.argv[1], sys.argv[2]
for sub in ("charts", "dashboards", "databases", "datasets"):          # giữ README.md
    shutil.rmtree(os.path.join(dest, sub), ignore_errors=True)
with zipfile.ZipFile(src_zip) as z:
    for name in z.namelist():
        if not name.endswith(".yaml"):
            continue
        rel = name.split("/", 1)[1]                                     # bỏ thư mục gốc có ngày giờ
        rel = re.sub(r"_\d+\.yaml$", ".yaml", rel)                      # bỏ hậu tố id: tên file ổn định
        path = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(z.read(name))
print("-> Da ghi", sum(1 for _, _, fs in os.walk(dest) for x in fs if x.endswith(".yaml")), "file YAML vao", dest + "/")
PY
echo "-> Xem thay doi:  git diff --stat superset/"
