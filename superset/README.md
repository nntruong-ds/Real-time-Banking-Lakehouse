# Dashboard dạng code (Superset)

Thư mục này là **bản xuất (export) của dashboard Superset**: database, 7 dataset, 12 chart và
1 dashboard, mỗi thứ một file YAML. Container `superset` nạp lại toàn bộ thư mục này mỗi lần
khởi động (`docker/superset/init.sh`), nên dashboard luôn giống hệt những gì có trong git.

- Sửa dashboard trên giao diện xong, lưu ngược về code:  `bash scripts/xuat_dashboard.sh`
- Mọi chart chỉ đọc các bảng `iceberg.marts.*` do dbt dựng: logic tính toán nằm ở dbt (có test),
  Superset chỉ hiển thị.
