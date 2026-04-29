# Olist Data Warehouse ETL Pipeline

Project này xây dựng pipeline **ETL + Data Warehouse + Dashboard** cho bộ dữ liệu Olist Brazilian E-commerce.

## 1. Ý tưởng chính

Luồng xử lý dữ liệu:

```text
CSV files
    ↓
Pentaho Data Integration
    ↓
Staging tables
    ↓
Dimension + Fact tables
    ↓
Python Streamlit Dashboard
```

Mục tiêu:

- Load dữ liệu CSV vào database staging.
- Làm sạch và biến đổi dữ liệu.
- Tạo mô hình Data Warehouse dạng đơn giản gồm dimension và fact.
- Trực quan hóa dữ liệu bằng dashboard.

---

## 2. Cấu trúc thư mục

```text
DATASET_ASS_DW - COPY
├── build_dim
├── build_fact
├── build_truncate
├── load_stagging
├── olist_dashboard
└── pipeline_job
```

Ý nghĩa:

| Thư mục | Nội dung |
|---|---|
| `load_stagging` | Pentaho transformations để load CSV vào staging |
| `build_dim` | Transformations build dimension tables |
| `build_fact` | Transformation build `fact_sales` |
| `build_truncate` | Transformation xóa dữ liệu cũ trước khi chạy lại |
| `pipeline_job` | Job tổng chạy toàn bộ pipeline |
| `olist_dashboard` | Source code dashboard Streamlit |

---

## 3. Database / DBMS

Project hiện tại đang chạy với **SQL Server**, nhưng ý tưởng có thể áp dụng cho các DBMS khác như:

- SQL Server
- PostgreSQL
- MySQL
- MariaDB

Chỉ cần chỉnh lại:

- JDBC driver trong Pentaho
- Database connection trong Pentaho
- Connection string trong Python dashboard
- SQL syntax nếu DBMS khác SQL Server

### Cấu hình SQL Server hiện tại

```text
Host: localhost
Port: 1433
Database: olist_dw
Username: pentaho_user
Password: your_password
```

### Pentaho Database Connection

Trong Pentaho Spoon, tạo connection:

```text
Connection name: sqlserver_olist
Connection type: MS SQL Server (Native)
Access: Native (JDBC)
Host Name: localhost
Database Name: olist_dw
Port Number: 1433
Username: pentaho_user
Password: your_password
```

Nếu dùng DBMS khác, chọn đúng loại connection:

```text
PostgreSQL / MySQL / MariaDB / SQL Server
```

và thay driver tương ứng.

---

## 4. Các bảng chính

### Staging tables

```text
stg_orders
stg_order_items
stg_customers
stg_products
stg_sellers
```

### Dimension tables

```text
dim_customer
dim_product
dim_seller
```

### Fact table

```text
fact_sales
```

`fact_sales` chứa các chỉ số chính:

```text
price
freight_value
total_amount
delivery_time_days
delivery_delay_days
is_late
load_date
batch_id
```

---

## 5. Logic nghiệp vụ chính

Bảng `fact_sales` được build từ:

```text
stg_orders + stg_order_items
```

Logic:

```text
Chỉ lấy order_status = delivered
Chỉ lấy price > 0
total_amount = price + freight_value
delivery_time_days = delivered_date - purchase_date
delivery_delay_days = delivered_date - estimated_date
is_late = 1 nếu giao trễ, ngược lại = 0
```

Dashboard join fact với dimension bằng natural key:

```text
fact_sales.customer_id → dim_customer.customer_id
fact_sales.product_id  → dim_product.product_id
fact_sales.seller_id   → dim_seller.seller_id
```

---

## 6. Chạy pipeline bằng Pentaho

Mở file job trong thư mục:

```text
pipeline_job
```

Job tổng nên chạy theo thứ tự:

```text
truncate_tables
→ load_stg_orders
→ load_stg_order_items
→ load_stg_customers
→ load_stg_products
→ load_stg_sellers
→ build_dim_customer
→ build_dim_product
→ build_dim_seller
→ build_fact_sales
```

Sau khi chạy, kiểm tra dữ liệu trong DBMS bằng câu lệnh đếm số dòng.

Ví dụ SQL Server:

```sql
SELECT COUNT(*) FROM fact_sales;
```

---

## 7. Dashboard Python

Dashboard nằm trong thư mục:

```text
olist_dashboard
```

### Cài thư viện

```bash
pip install streamlit pandas sqlalchemy pyodbc plotly
```

Nếu dùng PostgreSQL:

```bash
pip install psycopg2-binary
```

Nếu dùng MySQL/MariaDB:

```bash
pip install pymysql
```

---

## 8. Cấu hình kết nối Python

### SQL Server

```python
from sqlalchemy import create_engine
import urllib.parse

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=olist_dw;"
    "UID=pentaho_user;"
    "PWD=your_password;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
```

### PostgreSQL

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://username:password@localhost:5432/olist_dw"
)
```

### MySQL / MariaDB

```python
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://username:password@localhost:3306/olist_dw"
)
```

---

## 9. Chạy dashboard

Trong terminal:

```bash
cd olist_dashboard
python -m streamlit run app.py
```

Sau đó mở:

```text
http://localhost:8501
```

Dashboard đọc dữ liệu từ:

```text
fact_sales
dim_customer
dim_product
dim_seller
```

---

## 10. Thư viện sử dụng

| Thư viện | Mục đích |
|---|---|
| `streamlit` | Làm dashboard web |
| `pandas` | Xử lý dữ liệu |
| `sqlalchemy` | Kết nối database |
| `pyodbc` | Kết nối SQL Server |
| `plotly` | Vẽ biểu đồ tương tác |
| `psycopg2-binary` | Kết nối PostgreSQL nếu dùng Postgres |
| `pymysql` | Kết nối MySQL/MariaDB nếu dùng MySQL |

---
