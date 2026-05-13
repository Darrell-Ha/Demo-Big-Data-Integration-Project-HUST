# 🛒 Retail Data Lakehouse Project

Dự án xây dựng nền tảng **Data Lakehouse** hiện đại cho lĩnh vực bán lẻ, áp dụng kiến trúc **Medallion** để quản lý dữ liệu từ lúc phát sinh đến khi phục vụ phân tích (BI).

## 🏗 Kiến trúc hệ thống
Hệ thống sử dụng **Apache Iceberg** làm table format cốt lõi, giúp mang lại khả năng ACID và Time Travel trên nền tảng Object Storage (MinIO).

* **Storage:** MinIO (S3 Compatible).
* **Table Format:** Apache Iceberg.
* **Metastore:** Hive Metastore (PostgreSQL backend).
* **Processing Engine:**
    * **Spark:** Chịu trách nhiệm Ingestion và xử lý Raw -> Bronze.
    * **Trino:** Chịu trách nhiệm thực thi các transform phức tạp (Silver/Gold) thông qua **dbt**.
* **Orchestration:** Apache Airflow.
* **Visualization:** Metabase.

Hiện tại, project đáp ứng được nhu cầu với data source là các file dữ liệu dump từ các hệ thống nguồn:

<img width="1124" height="515" alt="image" src="https://github.com/user-attachments/assets/231c18a2-ff5e-4d21-810c-6f02b458f146" />


## 📂 Cấu trúc thư mục
* `infras-docker/`: Chứa toàn bộ cấu hình hạ tầng (Spark Cluster, Trino, Hive, MinIO).
* `airflow/`: Chứa mã nguồn điều phối (DAGs), dbt models và các script ingestion.

## 🚀 Hướng dẫn triển khai nhanh

### Bước 1: Khởi tạo hạ tầng
```bash
cd infras-docker
docker compose up -d
```

* Đợi khoảng 1-2 phút để Hive Metastore và Trino sẵn sàng.

### Bước 2: Khởi chạy Airflow
```bash
cd ../airflow
# Build custom image chứa các thư viện cần thiết
docker compose build 
docker compose up -d
```

### Bước 3: Truy cập UI

Airflow: http://localhost:8080 (admin/admin)

MinIO: http://localhost:19001 (minio/minio123)

Metabase: http://localhost:3000
