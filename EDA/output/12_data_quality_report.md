# Data Quality Report - Staging Layer

## 1. Tổng quan dữ liệu

Sau khi load dữ liệu vào staging, hệ thống có các bảng chính sau:

| table_name      |   row_count |   column_count |
|:----------------|------------:|---------------:|
| stg_orders      |       99441 |              8 |
| stg_order_items |      112650 |              7 |
| stg_customers   |       99441 |              5 |
| stg_products    |       32951 |              9 |
| stg_sellers     |        3095 |              4 |

## 2. Kết quả profiling chính

### 2.1 Missing values

Các vấn đề missing values đáng chú ý:

- `stg_orders.order_delivered_customer_date` thiếu 2965 dòng, tương đương 2.9817%.
- `stg_products.product_category_name` thiếu 610 dòng, tương đương 1.8512%.
- Các bảng `stg_customers` và `stg_sellers` không có missing values ở city/state.

### 2.2 Duplicate keys

Không phát hiện duplicate key ở các bảng chính:

- `stg_orders.order_id`
- `stg_order_items.order_id + order_item_id`
- `stg_customers.customer_id`
- `stg_products.product_id`
- `stg_sellers.seller_id`

Điều này giúp giảm nguy cơ nhân dòng khi build dimension và fact.

### 2.3 Order status

Phân phối trạng thái đơn hàng cho thấy có 96478 đơn `delivered`, chiếm khoảng 97.02% tổng số đơn.

Vì mục tiêu của `fact_sales` là phân tích doanh thu và hiệu suất giao hàng thực tế, pipeline chỉ giữ các đơn có trạng thái `delivered`.

### 2.4 Money quality

Không phát hiện lỗi nghiêm trọng ở `price` và `freight_value`:

- Không có `price` null.
- Không có `freight_value` null.
- Không có `price <= 0`.
- Không có `freight_value < 0`.

Do đó không cần clean trực tiếp nhóm cột tiền, nhưng pipeline vẫn giữ điều kiện `price > 0` như một rule phòng thủ.

### 2.5 Date quality

Không phát hiện trường hợp `delivery_time_days < 0`. Tuy nhiên có 43 đơn có thời gian giao hàng lớn hơn 120 ngày.

Các dòng này được xem là outlier vận hành và không bị xóa trong ETL vì chưa đủ bằng chứng coi là dữ liệu lỗi.

### 2.6 Relationship check

Không phát hiện lỗi relationship giữa các bảng staging:

- `order_items` đều có `order` tương ứng.
- `orders` đều có `customer` tương ứng.
- `order_items` đều có `product` tương ứng.
- `order_items` đều có `seller` tương ứng.

Điều này cho thấy dữ liệu đủ tốt để build `dim_customer`, `dim_product`, `dim_seller` và `fact_sales`.

### 2.7 Location insight

Khách hàng tập trung nhiều nhất ở bang `SP` với 41746 khách hàng.

Seller cũng tập trung nhiều nhất ở bang `SP` với 1849 seller.

Đây là insight hữu ích cho dashboard phân tích doanh thu theo khu vực.

## 3. Cleaning decision matrix

| detected_issue                     | evidence                  | decision                                                 | where_to_handle                | etl_rule                                                                       | reason                                                                                                         | priority   |
|:-----------------------------------|:--------------------------|:---------------------------------------------------------|:-------------------------------|:-------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------|:-----------|
| Missing product category           | 610 rows (1.8512%)        | Fill missing values with 'Unknown'                       | build_dim_product.ktr          | product_category_name NULL → 'Unknown'                                         | Giữ lại sản phẩm để không mất dữ liệu bán hàng, đồng thời vẫn có nhóm Unknown cho dashboard.                   | High       |
| Missing delivered customer date    | 2965 rows (2.9817%)       | Do not impute. Exclude from delivery metrics in fact.    | build_fact_sales.ktr           | WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL | Ngày giao thực tế không nên tự điền vì sẽ làm sai delivery_time_days và delivery_delay_days.                   | High       |
| Orders with non-delivered status   | 2963 rows (2.9797%)       | Keep only delivered orders for fact_sales                | build_fact_sales.ktr           | WHERE order_status = 'delivered'                                               | Dashboard phân tích doanh thu và giao hàng thực tế nên chỉ lấy đơn đã giao.                                    | High       |
| Invalid money values               | 0 invalid/null money rows | No actual cleaning needed. Keep safety filter price > 0. | build_fact_sales.ktr           | WHERE price > 0                                                                | Price/freight_value hiện tại sạch, nhưng giữ filter để phòng dữ liệu lỗi khi pipeline chạy lại với nguồn khác. | Medium     |
| Very long delivery time > 120 days | 43 rows                   | Keep records, mark as outlier for analysis               | EDA/reporting, not ETL removal | No deletion                                                                    | Đây có thể là trường hợp vận hành đặc biệt. Không xóa khi chưa có bằng chứng là dữ liệu lỗi.                   | Low        |
| Duplicate business keys            | 0 duplicate rows          | No duplicate removal needed                              | No action required             | Optional SELECT DISTINCT in dimension build                                    | Profiling không phát hiện duplicate key ở các bảng staging chính.                                              | Low        |
| Unmatched relationship keys        | 0 unmatched keys          | No relationship repair needed                            | No action required             | Normal joins are safe                                                          | Các khóa order_id, customer_id, product_id, seller_id đều match tốt giữa các bảng staging.                     | Low        |

## 4. Kết luận

Kết quả profiling cho thấy dữ liệu staging tương đối sạch: không có duplicate key, không có lỗi relationship, và không có giá trị tiền bất hợp lệ.

Các xử lý chính cần thực hiện trong ETL gồm:

1. Fill `product_category_name` bị thiếu thành `Unknown` trong `dim_product`.
2. Chỉ lấy đơn `delivered` khi build `fact_sales`.
3. Không tự điền ngày giao hàng bị thiếu; chỉ tính delivery metrics cho các đơn có đủ ngày.
4. Giữ filter `price > 0` như rule phòng thủ.
5. Ghi nhận các đơn giao hàng trên 120 ngày là outlier nhưng không xóa.
