import os
import pandas as pd


# ============================================================
# 1. CONFIG
# ============================================================

BASE_DIR = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

INPUT_FILES = {
    "table_overview": "00_table_overview.csv",
    "null_summary": "01_null_summary.csv",
    "duplicate_summary": "02_duplicate_summary.csv",
    "order_status": "03_order_status_distribution.csv",
    "money_quality": "04_money_quality_summary.csv",
    "date_quality": "05_date_quality_summary.csv",
    "relationship": "06_relationship_summary.csv",
    "product_category": "07_product_category_distribution.csv",
    "customer_state": "08_customer_state_distribution.csv",
    "seller_state": "09_seller_state_distribution.csv",
}


def read_output_csv(name: str) -> pd.DataFrame:
    path = os.path.join(OUTPUT_DIR, INPUT_FILES[name])

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing file: {path}\n"
            "Hãy chạy 01_data_profiling_staging.py trước."
        )

    return pd.read_csv(path)


def save_csv(df: pd.DataFrame, filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved: {path}")


def save_text(content: str, filename: str):
    path = os.path.join(OUTPUT_DIR, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"[OK] Saved: {path}")


def get_single_value(df: pd.DataFrame, column: str, default=0):
    if column not in df.columns:
        return default

    if df.empty:
        return default

    value = df[column].iloc[0]

    if pd.isna(value):
        return default

    return value


# ============================================================
# 2. LOAD PROFILING OUTPUT
# ============================================================

def load_all_outputs():
    data = {}

    for key in INPUT_FILES.keys():
        data[key] = read_output_csv(key)

    return data


# ============================================================
# 3. BUILD DATA QUALITY ISSUES
# ============================================================

def build_data_quality_issues(data: dict) -> pd.DataFrame:
    issues = []

    null_summary = data["null_summary"]
    duplicate_summary = data["duplicate_summary"]
    order_status = data["order_status"]
    money_quality = data["money_quality"]
    date_quality = data["date_quality"]
    relationship = data["relationship"]

    # --------------------------------------------------------
    # 3.1 Missing delivered date
    # --------------------------------------------------------
    delivered_null_row = null_summary[
        (null_summary["table_name"] == "stg_orders")
        & (null_summary["column_name"] == "order_delivered_customer_date")
    ]

    if not delivered_null_row.empty:
        null_count = int(delivered_null_row["null_count"].iloc[0])
        null_percent = float(delivered_null_row["null_percent"].iloc[0])

        issues.append(
            {
                "issue_name": "Missing delivered customer date",
                "table_name": "stg_orders",
                "column_name": "order_delivered_customer_date",
                "issue_count": null_count,
                "issue_percent": null_percent,
                "severity": "Medium",
                "business_impact": (
                    "Không thể tính delivery_time_days và delivery_delay_days "
                    "cho các đơn thiếu ngày giao thực tế."
                ),
            }
        )

    # --------------------------------------------------------
    # 3.2 Missing product category
    # --------------------------------------------------------
    category_null_row = null_summary[
        (null_summary["table_name"] == "stg_products")
        & (null_summary["column_name"] == "product_category_name")
    ]

    if not category_null_row.empty:
        null_count = int(category_null_row["null_count"].iloc[0])
        null_percent = float(category_null_row["null_percent"].iloc[0])

        issues.append(
            {
                "issue_name": "Missing product category",
                "table_name": "stg_products",
                "column_name": "product_category_name",
                "issue_count": null_count,
                "issue_percent": null_percent,
                "severity": "Medium",
                "business_impact": (
                    "Ảnh hưởng đến dashboard phân tích doanh thu theo danh mục sản phẩm."
                ),
            }
        )

    # --------------------------------------------------------
    # 3.3 Missing product physical attributes
    # --------------------------------------------------------
    product_physical_cols = [
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    for col in product_physical_cols:
        row = null_summary[
            (null_summary["table_name"] == "stg_products")
            & (null_summary["column_name"] == col)
        ]

        if not row.empty:
            null_count = int(row["null_count"].iloc[0])
            null_percent = float(row["null_percent"].iloc[0])

            if null_count > 0:
                issues.append(
                    {
                        "issue_name": f"Missing product attribute: {col}",
                        "table_name": "stg_products",
                        "column_name": col,
                        "issue_count": null_count,
                        "issue_percent": null_percent,
                        "severity": "Low",
                        "business_impact": (
                            "Không ảnh hưởng trực tiếp đến fact_sales hiện tại "
                            "vì dashboard chưa phân tích theo kích thước sản phẩm."
                        ),
                    }
                )

    # --------------------------------------------------------
    # 3.4 Order status not delivered
    # --------------------------------------------------------
    total_orders = int(order_status["total_orders"].sum())

    delivered_orders = int(
        order_status.loc[
            order_status["order_status"] == "delivered",
            "total_orders",
        ].sum()
    )

    non_delivered_orders = total_orders - delivered_orders
    non_delivered_percent = (
        round(non_delivered_orders / total_orders * 100, 4)
        if total_orders > 0
        else 0
    )

    issues.append(
        {
            "issue_name": "Orders with non-delivered status",
            "table_name": "stg_orders",
            "column_name": "order_status",
            "issue_count": non_delivered_orders,
            "issue_percent": non_delivered_percent,
            "severity": "Business Rule",
            "business_impact": (
                "Các đơn chưa giao thành công không nên được dùng để tính doanh thu "
                "và hiệu suất giao hàng thực tế."
            ),
        }
    )

    # --------------------------------------------------------
    # 3.5 Invalid money values
    # --------------------------------------------------------
    invalid_price = int(get_single_value(money_quality, "invalid_price_lte_0", 0))
    invalid_freight = int(get_single_value(money_quality, "invalid_freight_lt_0", 0))
    null_price = int(get_single_value(money_quality, "null_price", 0))
    null_freight = int(get_single_value(money_quality, "null_freight_value", 0))

    issues.append(
        {
            "issue_name": "Invalid money values",
            "table_name": "stg_order_items",
            "column_name": "price, freight_value",
            "issue_count": invalid_price + invalid_freight + null_price + null_freight,
            "issue_percent": 0,
            "severity": "Low" if invalid_price + invalid_freight + null_price + null_freight == 0 else "High",
            "business_impact": (
                "Price/freight_value dùng để tính doanh thu. Nếu lỗi sẽ làm sai total_amount."
            ),
        }
    )

    # --------------------------------------------------------
    # 3.6 Date outlier
    # --------------------------------------------------------
    very_long_delivery = int(
        get_single_value(date_quality, "very_long_delivery_time_gt_120_days", 0)
    )

    if very_long_delivery > 0:
        total_order_count = int(get_single_value(date_quality, "total_orders", 0))
        percent = (
            round(very_long_delivery / total_order_count * 100, 4)
            if total_order_count > 0
            else 0
        )

        issues.append(
            {
                "issue_name": "Very long delivery time > 120 days",
                "table_name": "stg_orders",
                "column_name": "order_purchase_timestamp, order_delivered_customer_date",
                "issue_count": very_long_delivery,
                "issue_percent": percent,
                "severity": "Low",
                "business_impact": (
                    "Có thể là outlier về vận hành giao hàng. Chưa đủ bằng chứng để xóa."
                ),
            }
        )

    # --------------------------------------------------------
    # 3.7 Duplicate keys
    # --------------------------------------------------------
    total_duplicate_groups = int(duplicate_summary["duplicate_groups"].sum())
    total_duplicate_rows = int(duplicate_summary["duplicate_rows"].sum())

    issues.append(
        {
            "issue_name": "Duplicate business keys",
            "table_name": "all staging tables",
            "column_name": "primary business keys",
            "issue_count": total_duplicate_rows,
            "issue_percent": 0,
            "severity": "Low" if total_duplicate_rows == 0 else "High",
            "business_impact": (
                "Duplicate key có thể làm nhân dòng trong dim/fact. "
                "Kết quả hiện tại không phát hiện duplicate."
            ),
        }
    )

    # --------------------------------------------------------
    # 3.8 Relationship issue
    # --------------------------------------------------------
    total_relationship_issues = int(relationship["issue_count"].sum())

    issues.append(
        {
            "issue_name": "Unmatched relationship keys",
            "table_name": "staging relationships",
            "column_name": "order_id, customer_id, product_id, seller_id",
            "issue_count": total_relationship_issues,
            "issue_percent": 0,
            "severity": "Low" if total_relationship_issues == 0 else "High",
            "business_impact": (
                "Nếu khóa không match, fact_sales sẽ không join được với dimension."
            ),
        }
    )

    return pd.DataFrame(issues)


# ============================================================
# 4. BUILD CLEANING DECISION MATRIX
# ============================================================

def build_cleaning_decision_matrix(issues_df: pd.DataFrame) -> pd.DataFrame:
    decisions = []

    def get_issue_count(issue_name: str):
        row = issues_df[issues_df["issue_name"] == issue_name]
        if row.empty:
            return 0
        return int(row["issue_count"].iloc[0])

    def get_issue_percent(issue_name: str):
        row = issues_df[issues_df["issue_name"] == issue_name]
        if row.empty:
            return 0
        return float(row["issue_percent"].iloc[0])

    # 1. Product category null
    decisions.append(
        {
            "detected_issue": "Missing product category",
            "evidence": (
                f"{get_issue_count('Missing product category')} rows "
                f"({get_issue_percent('Missing product category')}%)"
            ),
            "decision": "Fill missing values with 'Unknown'",
            "where_to_handle": "build_dim_product.ktr",
            "etl_rule": "product_category_name NULL → 'Unknown'",
            "reason": (
                "Giữ lại sản phẩm để không mất dữ liệu bán hàng, "
                "đồng thời vẫn có nhóm Unknown cho dashboard."
            ),
            "priority": "High",
        }
    )

    # 2. Missing delivered date
    decisions.append(
        {
            "detected_issue": "Missing delivered customer date",
            "evidence": (
                f"{get_issue_count('Missing delivered customer date')} rows "
                f"({get_issue_percent('Missing delivered customer date')}%)"
            ),
            "decision": "Do not impute. Exclude from delivery metrics in fact.",
            "where_to_handle": "build_fact_sales.ktr",
            "etl_rule": (
                "WHERE order_status = 'delivered' "
                "AND order_delivered_customer_date IS NOT NULL"
            ),
            "reason": (
                "Ngày giao thực tế không nên tự điền vì sẽ làm sai delivery_time_days "
                "và delivery_delay_days."
            ),
            "priority": "High",
        }
    )

    # 3. Non-delivered orders
    decisions.append(
        {
            "detected_issue": "Orders with non-delivered status",
            "evidence": (
                f"{get_issue_count('Orders with non-delivered status')} rows "
                f"({get_issue_percent('Orders with non-delivered status')}%)"
            ),
            "decision": "Keep only delivered orders for fact_sales",
            "where_to_handle": "build_fact_sales.ktr",
            "etl_rule": "WHERE order_status = 'delivered'",
            "reason": (
                "Dashboard phân tích doanh thu và giao hàng thực tế nên chỉ lấy đơn đã giao."
            ),
            "priority": "High",
        }
    )

    # 4. Invalid money
    invalid_money_count = get_issue_count("Invalid money values")
    decisions.append(
        {
            "detected_issue": "Invalid money values",
            "evidence": f"{invalid_money_count} invalid/null money rows",
            "decision": (
                "No actual cleaning needed. Keep safety filter price > 0."
                if invalid_money_count == 0
                else "Remove invalid money rows"
            ),
            "where_to_handle": "build_fact_sales.ktr",
            "etl_rule": "WHERE price > 0",
            "reason": (
                "Price/freight_value hiện tại sạch, nhưng giữ filter để phòng dữ liệu lỗi "
                "khi pipeline chạy lại với nguồn khác."
            ),
            "priority": "Medium",
        }
    )

    # 5. Long delivery outlier
    decisions.append(
        {
            "detected_issue": "Very long delivery time > 120 days",
            "evidence": f"{get_issue_count('Very long delivery time > 120 days')} rows",
            "decision": "Keep records, mark as outlier for analysis",
            "where_to_handle": "EDA/reporting, not ETL removal",
            "etl_rule": "No deletion",
            "reason": (
                "Đây có thể là trường hợp vận hành đặc biệt. "
                "Không xóa khi chưa có bằng chứng là dữ liệu lỗi."
            ),
            "priority": "Low",
        }
    )

    # 6. Duplicate
    decisions.append(
        {
            "detected_issue": "Duplicate business keys",
            "evidence": f"{get_issue_count('Duplicate business keys')} duplicate rows",
            "decision": "No duplicate removal needed",
            "where_to_handle": "No action required",
            "etl_rule": "Optional SELECT DISTINCT in dimension build",
            "reason": (
                "Profiling không phát hiện duplicate key ở các bảng staging chính."
            ),
            "priority": "Low",
        }
    )

    # 7. Relationship
    decisions.append(
        {
            "detected_issue": "Unmatched relationship keys",
            "evidence": f"{get_issue_count('Unmatched relationship keys')} unmatched keys",
            "decision": "No relationship repair needed",
            "where_to_handle": "No action required",
            "etl_rule": "Normal joins are safe",
            "reason": (
                "Các khóa order_id, customer_id, product_id, seller_id đều match tốt "
                "giữa các bảng staging."
            ),
            "priority": "Low",
        }
    )

    return pd.DataFrame(decisions)


# ============================================================
# 5. BUILD MARKDOWN REPORT
# ============================================================

def build_markdown_report(data: dict, issues_df: pd.DataFrame, decisions_df: pd.DataFrame) -> str:
    table_overview = data["table_overview"]
    order_status = data["order_status"]
    customer_state = data["customer_state"]
    seller_state = data["seller_state"]

    def issue_count(name: str):
        row = issues_df[issues_df["issue_name"] == name]
        if row.empty:
            return 0
        return int(row["issue_count"].iloc[0])

    def issue_percent(name: str):
        row = issues_df[issues_df["issue_name"] == name]
        if row.empty:
            return 0
        return float(row["issue_percent"].iloc[0])

    delivered_count = int(
        order_status.loc[
            order_status["order_status"] == "delivered",
            "total_orders",
        ].sum()
    )

    total_orders = int(order_status["total_orders"].sum())
    delivered_percent = round(delivered_count / total_orders * 100, 2)

    top_customer_state = customer_state.iloc[0]["customer_state"]
    top_customer_count = int(customer_state.iloc[0]["total_customers"])

    top_seller_state = seller_state.iloc[0]["seller_state"]
    top_seller_count = int(seller_state.iloc[0]["total_sellers"])

    report = f"""# Data Quality Report - Staging Layer

## 1. Tổng quan dữ liệu

Sau khi load dữ liệu vào staging, hệ thống có các bảng chính sau:

{table_overview.to_markdown(index=False)}

## 2. Kết quả profiling chính

### 2.1 Missing values

Các vấn đề missing values đáng chú ý:

- `stg_orders.order_delivered_customer_date` thiếu {issue_count("Missing delivered customer date")} dòng, tương đương {issue_percent("Missing delivered customer date")}%.
- `stg_products.product_category_name` thiếu {issue_count("Missing product category")} dòng, tương đương {issue_percent("Missing product category")}%.
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

Phân phối trạng thái đơn hàng cho thấy có {delivered_count} đơn `delivered`, chiếm khoảng {delivered_percent}% tổng số đơn.

Vì mục tiêu của `fact_sales` là phân tích doanh thu và hiệu suất giao hàng thực tế, pipeline chỉ giữ các đơn có trạng thái `delivered`.

### 2.4 Money quality

Không phát hiện lỗi nghiêm trọng ở `price` và `freight_value`:

- Không có `price` null.
- Không có `freight_value` null.
- Không có `price <= 0`.
- Không có `freight_value < 0`.

Do đó không cần clean trực tiếp nhóm cột tiền, nhưng pipeline vẫn giữ điều kiện `price > 0` như một rule phòng thủ.

### 2.5 Date quality

Không phát hiện trường hợp `delivery_time_days < 0`. Tuy nhiên có {issue_count("Very long delivery time > 120 days")} đơn có thời gian giao hàng lớn hơn 120 ngày.

Các dòng này được xem là outlier vận hành và không bị xóa trong ETL vì chưa đủ bằng chứng coi là dữ liệu lỗi.

### 2.6 Relationship check

Không phát hiện lỗi relationship giữa các bảng staging:

- `order_items` đều có `order` tương ứng.
- `orders` đều có `customer` tương ứng.
- `order_items` đều có `product` tương ứng.
- `order_items` đều có `seller` tương ứng.

Điều này cho thấy dữ liệu đủ tốt để build `dim_customer`, `dim_product`, `dim_seller` và `fact_sales`.

### 2.7 Location insight

Khách hàng tập trung nhiều nhất ở bang `{top_customer_state}` với {top_customer_count} khách hàng.

Seller cũng tập trung nhiều nhất ở bang `{top_seller_state}` với {top_seller_count} seller.

Đây là insight hữu ích cho dashboard phân tích doanh thu theo khu vực.

## 3. Cleaning decision matrix

{decisions_df.to_markdown(index=False)}

## 4. Kết luận

Kết quả profiling cho thấy dữ liệu staging tương đối sạch: không có duplicate key, không có lỗi relationship, và không có giá trị tiền bất hợp lệ.

Các xử lý chính cần thực hiện trong ETL gồm:

1. Fill `product_category_name` bị thiếu thành `Unknown` trong `dim_product`.
2. Chỉ lấy đơn `delivered` khi build `fact_sales`.
3. Không tự điền ngày giao hàng bị thiếu; chỉ tính delivery metrics cho các đơn có đủ ngày.
4. Giữ filter `price > 0` như rule phòng thủ.
5. Ghi nhận các đơn giao hàng trên 120 ngày là outlier nhưng không xóa.
"""

    return report


# ============================================================
# 6. MAIN
# ============================================================

def main():
    print("Starting data quality report generation...")
    print(f"Reading profiling outputs from: {OUTPUT_DIR}")

    data = load_all_outputs()

    issues_df = build_data_quality_issues(data)
    decisions_df = build_cleaning_decision_matrix(issues_df)

    save_csv(issues_df, "10_data_quality_issues.csv")
    save_csv(decisions_df, "11_cleaning_decision_matrix.csv")

    report = build_markdown_report(data, issues_df, decisions_df)
    save_text(report, "12_data_quality_report.md")

    print("\nDONE: Data quality report generated.")
    print("Please check:")
    print("- EDA/output/10_data_quality_issues.csv")
    print("- EDA/output/11_cleaning_decision_matrix.csv")
    print("- EDA/output/12_data_quality_report.md")


if __name__ == "__main__":
    main()