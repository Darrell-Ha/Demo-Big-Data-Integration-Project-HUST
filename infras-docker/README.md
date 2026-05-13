# 🏗 Data Infrastructure Cluster

Thư mục này chứa cấu hình Docker Compose cho toàn bộ hạ tầng xử lý dữ liệu.

## 🔌 Network Configuration
Dự án sử dụng một mạng nội bộ riêng:
* **Subnet:** `172.19.0.0/16`
* **Domain:** `retail-lake.domain`

## 📊 Chi tiết các Service

### 1. Storage & Metastore
* **MinIO:** Đóng vai trò S3 Storage. Các bucket chính: `bronze`, `silver`, `gold`.
* **Hive Metastore:** Lưu trữ metadata của các bảng Iceberg. Dữ liệu thực tế nằm trên Postgres.

### 2. Computing Engines
* **Spark Cluster:** Gồm 1 Master và 2 Workers. Sử dụng Dockerfile tùy chỉnh `spark_311.Dockerfile` để cài đặt sẵn Python 3.11 và các dependencies.
* **Trino:** SQL Engine cực nhanh để truy vấn Iceberg. Cấu hình các catalog nằm trong `trino-engine/etc-conf/catalog`.

### 3. Analytics
* **Metabase:** Kết nối trực tiếp với Trino để thực hiện Dashboarding.

## 🛠 Lưu ý về Resource
Để hệ thống chạy ổn định, khuyến nghị cấp phát cho Docker ít nhất:
* **CPU:** 4 Cores
* **RAM:** 8GB - 12GB