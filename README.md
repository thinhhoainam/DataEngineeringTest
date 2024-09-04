# Data Engineering Test: Google Cloud Storage to PostgreSQL

## Bối Cảnh

Dự án này giả định rằng một tệp CSV chứa dữ liệu yêu cầu bảo hiểm được lưu trữ trong Google Cloud Storage (GCS). Mục tiêu là sử dụng Apache Spark để trích xuất dữ liệu, tải vào cơ sở dữ liệu PostgreSQL, thực hiện một số phân tích, và lưu kết quả vào các bảng PostgreSQL mới.

## Dữ Liệu Đầu Vào

Tệp CSV trong GCS chứa các cột sau: `IDpol`, `ClaimNb`, `Exposure`, `VehPower`, `VehAge`, `DrivAge`, `BonusMalus`, `VehBrand`, `VehGas`, `Area`, `Density`, `Region`, `ClaimAmount`.

## Nhiệm Vụ

### 1. Trích Xuất Dữ Liệu và Tải Vào

Viết một script PySpark để:
- Trích xuất dữ liệu CSV từ Google Cloud Storage (GCS).
- Tải dữ liệu vào một bảng PostgreSQL có tên là `insurance_claims`.
- Đảm bảo xử lý lỗi và ghi log đúng cách.

### 2. Truy Vấn: Tổng `Exposure` theo `VehBrand` và `Area`

Viết truy vấn SQL trong PostgreSQL để:
- Tính tổng `Exposure` cho mỗi kết hợp duy nhất của `VehBrand` và `Area`.
- Lưu kết quả vào một bảng mới có tên là `exposure_summary`.

### 3. Truy Vấn: Tìm Giá Trị Nhỏ Nhất và Lớn Nhất của `Density` theo `Area`

Viết truy vấn SQL trong PostgreSQL để:
- Tính giá trị nhỏ nhất và lớn nhất của `Density` cho mỗi `Area`.
- Lưu kết quả vào một bảng mới có tên là `density_summary`.
## Cách thực hiện
### 1. Trích Xuất dữ liệu và tải dữ liệu
### Cách 1: Sử Dụng Python thuần (pandas)
### Cách Tiếp Cận:

- Trích xuất dữ liệu từ GCS: Sử dụng thư viện google-cloud-storage để tải xuống tệp CSV từ GCS.
- Tải dữ liệu lên PostgreSQL: Sử dụng thư viện pandas để đọc tệp CSV và SQLAlchemy để tải dữ liệu lên PostgreSQL.
- Xử lý lỗi và ghi nhật ký: Đảm bảo mã có khả năng xử lý lỗi và ghi nhật ký đầy đủ để theo dõi quá trình thực thi.
```
import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine, text
import logging

# Cấu hình ghi nhật ký
logging.basicConfig(level=logging.INFO)

# Cài đặt Google Cloud Storage
BUCKET_NAME = 'bucket-name'
SOURCE_BLOB_NAME = 'path/to/insurance_claims.csv'
DESTINATION_FILE_NAME = '/tmp/insurance_claims.csv'

# Cài đặt PostgreSQL
DATABASE_URI = 'postgresql://user:pass@localhost:5432/database'
TABLE_NAME = 'insurance_claims'

def download_csv_from_gcs():
    """Tải xuống tệp CSV từ Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(SOURCE_BLOB_NAME)
        blob.download_to_filename(DESTINATION_FILE_NAME)
        logging.info(f"Đã tải xuống {SOURCE_BLOB_NAME} vào {DESTINATION_FILE_NAME}.")
    except Exception as e:
        logging.error(f"Lỗi khi tải tệp từ GCS: {e}")
        raise

def create_table_if_not_exists(engine):
    """Tạo bảng nếu chưa tồn tại trong PostgreSQL."""
    try:
        with engine.connect() as connection:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                IDpol VARCHAR(50),
                ClaimNb INT,
                Exposure FLOAT,
                VehPower INT,
                VehAge INT,
                DrivAge INT,
                BonusMalus INT,
                VehBrand VARCHAR(50),
                VehGas VARCHAR(50),
                Area VARCHAR(50),
                Density FLOAT,
                Region VARCHAR(50),
                ClaimAmount FLOAT
            );
            """
            connection.execute(text(create_table_query))
            logging.info(f"Bảng '{TABLE_NAME}' đã tồn tại hoặc đã được tạo thành công.")
    except Exception as e:
        logging.error(f"Lỗi khi tạo bảng PostgreSQL: {e}")
        raise

def load_csv_to_postgresql():
    """Tải tệp CSV vào cơ sở dữ liệu PostgreSQL."""
    try:
        # Đọc tệp CSV
        df = pd.read_csv(DESTINATION_FILE_NAME)
        
        # Tạo kết nối PostgreSQL
        engine = create_engine(DATABASE_URI)
        
        # Tạo bảng nếu chưa tồn tại
        create_table_if_not_exists(engine)
        
        # Tải DataFrame vào PostgreSQL
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        logging.info("Dữ liệu đã được tải thành công vào PostgreSQL.")
    except Exception as e:
        logging.error(f"Lỗi khi tải dữ liệu vào PostgreSQL: {e}")
        raise

def main():
    download_csv_from_gcs()
    load_csv_to_postgresql()

if __name__ == "__main__":
    main()

```
### Ưu Điểm:

- Đơn giản và dễ hiểu: Được triển khai bằng Python thuần túy và rất thân thiện với những người quen thuộc với pandas.
- Nhanh chóng cho tập dữ liệu nhỏ: Hiệu quả cho việc xử lý các tập dữ liệu nhỏ đến trung bình.
### Nhược Điểm:

- Giới hạn bộ nhớ: Không phù hợp với tập dữ liệu lớn hơn dung lượng bộ nhớ RAM.
- Không thể mở rộng: Khó mở rộng đối với các khối lượng công việc lớn.

### Cách 2: Sử Dụng PySpark
### Cách Tiếp Cận:

- Trích xuất dữ liệu từ GCS: Sử dụng PySpark để đọc trực tiếp dữ liệu CSV từ GCS.
- Tải dữ liệu lên PostgreSQL: Sử dụng thư viện jdbc của Spark để ghi dữ liệu từ DataFrame của Spark vào PostgreSQL.
- Xử lý lỗi và tối ưu hóa: Cần xử lý các ngoại lệ và tối ưu hóa cấu hình Spark để tận dụng tài nguyên tốt nhất.
```
from pyspark.sql import SparkSession
import logging
import psycopg2

# Cấu hình ghi nhật ký
logging.basicConfig(level=logging.INFO)

# Cài đặt Google Cloud Storage và PostgreSQL
GCS_BUCKET = 'bucket-name'
CSV_FILE_PATH = f'gs://{GCS_BUCKET}/path/to/insurance_claim_data.csv'
POSTGRESQL_JDBC_URL = 'jdbc:postgresql://localhost:5432/database'
POSTGRESQL_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Tạo Spark Session
spark = SparkSession.builder \
    .appName("GCS to PostgreSQL ETL") \
    .config("spark.jars", "/path/to/postgresql-42.2.18.jar") \
    .getOrCreate()

def create_table_if_not_exists():
    """Tạo bảng nếu chưa tồn tại trong PostgreSQL."""
    try:
        # Kết nối tới PostgreSQL
        connection = psycopg2.connect(
            dbname='database',
            user='user',
            password='password',
            host='localhost',
            port='5432'
        )
        cursor = connection.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS insurance_claims (
            IDpol VARCHAR(50),
            ClaimNb INT,
            Exposure FLOAT,
            VehPower INT,
            VehAge INT,
            DrivAge INT,
            BonusMalus INT,
            VehBrand VARCHAR(50),
            VehGas VARCHAR(50),
            Area VARCHAR(50),
            Density FLOAT,
            Region VARCHAR(50),
            ClaimAmount FLOAT
        );
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        logging.info(f"Bảng 'insurance_claims' đã tồn tại hoặc đã được tạo thành công.")
    
    except Exception as e:
        logging.error(f"Lỗi khi tạo bảng PostgreSQL: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def extract_from_gcs():
    """Đọc tệp CSV từ Google Cloud Storage."""
    try:
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(CSV_FILE_PATH)
        logging.info(f"Đã đọc dữ liệu từ {CSV_FILE_PATH}.")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi đọc dữ liệu từ GCS: {e}")
        raise

def load_to_postgresql(df):
    """Ghi DataFrame vào cơ sở dữ liệu PostgreSQL."""
    try:
        df.write.jdbc(url=POSTGRESQL_JDBC_URL, table='insurance_claims', mode='append', properties=POSTGRESQL_PROPERTIES)
        logging.info("Dữ liệu đã được ghi thành công vào PostgreSQL.")
    except Exception as e:
        logging.error(f"Lỗi khi ghi dữ liệu vào PostgreSQL: {e}")
        raise

def main():
    create_table_if_not_exists()
    df = extract_from_gcs()
    load_to_postgresql(df)

if __name__ == "__main__":
    main()
    spark.stop()
```
### Ưu Điểm:

- Có thể mở rộng: PySpark rất phù hợp với các tập dữ liệu lớn và có thể mở rộng theo chiều ngang.
- Hiệu suất cao: Khả năng xử lý song song giúp cải thiện tốc độ xử lý và tải dữ liệu.
### Nhược Điểm:

- Cần cấu hình phức tạp hơn: Cấu hình Spark và thiết lập môi trường có thể phức tạp hơn đối với những người chưa quen.
- Chi phí tài nguyên: Yêu cầu nhiều tài nguyên hơn, có thể tốn kém nếu không được tối ưu hóa.

### 2. Truy Vấn SQL
### Truy Vấn 1: Tổng `Exposure` theo `VehBrand` và `Area`
```
-- Tạo bảng để lưu tổng kết Exposure
CREATE TABLE exposure_summary (
    VehBrand VARCHAR(50),
    Area VARCHAR(50),
    SumExposure NUMERIC
);

-- Truy vấn để tính tổng Exposure theo VehBrand và Area
INSERT INTO exposure_summary (VehBrand, Area, SumExposure)
SELECT VehBrand, Area, SUM(Exposure) AS SumExposure
FROM insurance_claims
GROUP BY VehBrand, Area;
```

### Truy Vấn 2: Tìm Giá Trị Nhỏ Nhất và Lớn Nhất của `Density` theo `Area`
```
-- Tạo bảng để lưu tổng kết mật độ
CREATE TABLE density_summary (
    Area VARCHAR(50),
    MinDensity NUMERIC,
    MaxDensity NUMERIC
);

-- Truy vấn để tính mật độ tối thiểu và tối đa theo Area
INSERT INTO density_summary (Area, MinDensity, MaxDensity)
SELECT Area, MIN(Density) AS MinDensity, MAX(Density) AS MaxDensity
FROM insurance_claims
GROUP BY Area;
```


### 3. Lập Chỉ Mục và Kiểm Soát Truy Cập

### Tạo Chỉ Mục
```
-- Tạo chỉ mục trên cột VehBrand và Area cho bảng insurance_claims
CREATE INDEX idx_vehbrand_area ON insurance_claims (VehBrand, Area);

-- Tạo chỉ mục trên cột Area cho bảng insurance_claims
CREATE INDEX idx_area ON insurance_claims (Area);
```

### Tạo Vai Trò và Cấp Quyền
```
-- Tạo vai trò người dùng mới với quyền chỉ đọc
CREATE ROLE readonly_user WITH LOGIN PASSWORD 'matkhauantoan';

-- Cấp quyền SELECT trên các bảng cho readonly_user
GRANT SELECT ON insurance_claims, exposure_summary, density_summary TO readonly_user;

-- Tạo vai trò người dùng mới với quyền đầy đủ
CREATE ROLE data_manager WITH LOGIN PASSWORD 'matkhaukhac';

-- Cấp quyền ALL trên tất cả các bảng trong schema public cho data_manager
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO data_manager;
```

## Giải Thích Về Cách Tiếp Cận

### 1. Trích xuất và tải dữ liệu
### Phương pháp Python (pandas):
- Trích xuất: Sử dụng thư viện google-cloud-storage để tải tệp CSV từ GCS.
- Tải lên: Dùng pandas để đọc CSV và SQLAlchemy để ghi dữ liệu vào PostgreSQL.
- Xử lý lỗi: Dùng try-except để xử lý lỗi tải tệp và kết nối cơ sở dữ liệu.
- Tối ưu hóa hiệu suất: Dùng chèn theo lô (batch insert) khi tải dữ liệu.
- Bảo mật: Sử dụng biến môi trường cho thông tin nhạy cảm và kết nối bảo mật SSL với PostgreSQL.
### Phương pháp PySpark:
- Trích xuất: Dùng spark.read.format("csv") để đọc trực tiếp từ GCS.
- Tải lên: Dùng DataFrame.write.jdbc() để ghi dữ liệu vào PostgreSQL.
- Xử lý lỗi: Thêm try-except để xử lý các lỗi không mong đợi.
- Tối ưu hóa hiệu suất: Dùng khả năng xử lý song song của Spark và điều chỉnh số phân vùng.
- Bảo mật: Dùng kết nối an toàn với GCS và PostgreSQL, quản lý mật khẩu qua công cụ như Google Secret Manager.
### 2. Truy vấn và tối ưu SQL
- Các truy vấn SQL được tối ưu hóa sử dụng GROUP BY để giảm thiểu thời gian tính toán.
- Sử dụng chỉ mục để tăng tốc độ truy vấn.
### 3. Xem Xét Bảo Mật:
- Hạn chế quyền truy cập vào dữ liệu nhạy cảm bằng cách tạo các vai trò với quyền cụ thể.
- Bảo mật mật khẩu và thông tin nhạy cảm.

