"""
Nạp dashboard dạng code (thư mục superset/ của repo) vào Superset, GHI ĐÈ bản đang có.

Vì sao không dùng lệnh "superset import-dashboards": lệnh đó chỉ ghi đè chính dashboard,
còn database / dataset / chart đã có thì GIỮ NGUYÊN. Nghĩa là sửa chart trong git rồi khởi
động lại, Superset vẫn hiện bản cũ. Ở đây gọi lần lượt 4 lệnh import của Superset, mỗi lệnh
ghi đè đúng loại đối tượng của nó, để bản trong git luôn thắng:
    database -> dataset -> chart -> dashboard

Chạy trong container (xem docker/superset/init.sh):
    python /app/import_dashboards.py /app/dashboards
"""

import os
import re
import sys

from superset.app import create_app

SRC = sys.argv[1] if len(sys.argv) > 1 else "/app/dashboards"

app = create_app()
with app.app_context():
    from flask import g
    from superset import security_manager
    from superset.commands.chart.importers.v1 import ImportChartsCommand
    from superset.commands.dashboard.importers.v1 import ImportDashboardsCommand
    from superset.commands.database.importers.v1 import ImportDatabasesCommand
    from superset.commands.dataset.importers.v1 import ImportDatasetsCommand

    g.user = security_manager.find_user(username=os.environ.get("SUPERSET_ADMIN_USER", "admin"))

    contents = {}
    for root, _, files in os.walk(SRC):
        for name in files:
            if name.endswith(".yaml"):
                path = os.path.join(root, name)
                with open(path, encoding="utf-8") as f:
                    contents[os.path.relpath(path, SRC)] = f.read()
    if "metadata.yaml" not in contents:
        sys.exit(f"[superset] Khong thay {SRC}/metadata.yaml")

    def as_type(model_type: str) -> dict:
        """Mỗi lệnh import kiểm tra 'type' trong metadata.yaml phải đúng loại của nó."""
        bundle = dict(contents)
        bundle["metadata.yaml"] = re.sub(r"(?m)^type: .*$", f"type: {model_type}",
                                         contents["metadata.yaml"])
        return bundle

    for command, model_type in ((ImportDatabasesCommand, "Database"),
                                (ImportDatasetsCommand, "SqlaTable"),
                                (ImportChartsCommand, "Slice"),
                                (ImportDashboardsCommand, "Dashboard")):
        command(as_type(model_type), overwrite=True).run()
        print(f"[superset] Nap {model_type}: xong", flush=True)
