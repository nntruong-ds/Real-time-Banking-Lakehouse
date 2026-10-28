"""
Cấu hình Superset cho môi trường học tập trên máy cá nhân.

- Metadata (người dùng, dashboard...) lưu trong SQLite ở volume superset-home: đủ cho một người
  dùng, đỡ một container Postgres. Production sẽ dùng Postgres/MySQL.
- Dashboard KHÔNG sống trong metadata này: nó nằm dạng YAML trong thư mục superset/ của repo
  và được nạp lại mỗi lần container khởi động (xem docker/superset/init.sh).
"""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db?check_same_thread=false"

ROW_LIMIT = 5000
SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_TIMEOUT = 120

# Giao diện tiếng Anh mặc định; có sẵn tiếng Việt? Superset chưa có -> giữ en.
BABEL_DEFAULT_LOCALE = "en"
