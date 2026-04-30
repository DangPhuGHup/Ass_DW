import os
import urllib.parse

import pandas as pd
import pyodbc
from sqlalchemy import create_engine


# ============================================================
# 1. CONFIG DATABASE CONNECTION
# ============================================================

DB_CONFIG = {
    "server": "localhost",
    "port": "1433",
    "database": "olist_dw",
    "username": "pentaho_user",
    "password": "Pentaho@12345",
}

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_sqlserver_driver():
    """
    Tự động chọn ODBC Driver 18 hoặc 17 nếu có.
    """
    drivers = pyodbc.drivers()

    if "ODBC Driver 18 for SQL Server" in drivers:
        return "ODBC Driver 18 for SQL Server"

    if "ODBC Driver 17 for SQL Server" in drivers:
        return "ODBC Driver 17 for SQL Server"

    raise RuntimeError(
        "Không tìm thấy ODBC Driver 17 hoặc 18 for SQL Server. "
        f"Driver hiện có: {drivers}"
    )


def get_engine():
    driver = get_sqlserver_driver()

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_CONFIG['server']},{DB_CONFIG['port']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine


def save_report(df, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved: {path}")


def read_sql(engine, query):
    return pd.read_sql_query(query, engine)


# ============================================================
# 2. TABLE CONFIG
# ============================================================

STAGING_TABLES = {
    "stg_orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "stg_order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    ],
    "stg_customers": [
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
    ],
    "stg_products": [
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
    "stg_sellers": [
        "seller_id",
        "seller_city",
        "seller_state",
    ],
}

DUPLICATE_KEYS = {
    "stg_orders": ["order_id"],
    "stg_order_items": ["order_id", "order_item_id"],
    "stg_customers": ["customer_id"],
    "stg_products": ["product_id"],
    "stg_sellers": ["seller_id"],
}


# ============================================================
# 3. CHECK 1: TABLE OVERVIEW
# ============================================================

def profile_table_overview(engine):
    print("\n=== 1. TABLE OVERVIEW ===")

    queries = []

    for table_name in STAGING_TABLES.keys():
        queries.append(
            f"""
            SELECT
                '{table_name}' AS table_name,
                COUNT(*) AS row_count
            FROM dbo.{table_name}
            """
        )

    row_count_query = "\nUNION ALL\n".join(queries)
    row_count_df = read_sql(engine, row_count_query)

    column_count_query = """
    SELECT
        TABLE_NAME AS table_name,
        COUNT(*) AS column_count
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME IN (
        'stg_orders',
        'stg_order_items',
        'stg_customers',
        'stg_products',
        'stg_sellers'
      )
    GROUP BY TABLE_NAME;
    """

    column_count_df = read_sql(engine, column_count_query)

    result = row_count_df.merge(column_count_df, on="table_name", how="left")
    save_report(result, "00_table_overview.csv")

    print(result)
    return result


# ============================================================
# 4. CHECK 2: NULL VALUES
# ============================================================

def profile_null_values(engine):
    print("\n=== 2. NULL VALUES ===")

    results = []

    for table_name, columns in STAGING_TABLES.items():
        total_query = f"SELECT COUNT(*) AS total_rows FROM dbo.{table_name};"
        total_rows = int(read_sql(engine, total_query)["total_rows"].iloc[0])

        for col in columns:
            query = f"""
            SELECT
                COUNT(*) AS null_count
            FROM dbo.{table_name}
            WHERE {col} IS NULL;
            """

            null_count = int(read_sql(engine, query)["null_count"].iloc[0])
            null_percent = (null_count / total_rows * 100) if total_rows > 0 else 0

            results.append(
                {
                    "table_name": table_name,
                    "column_name": col,
                    "total_rows": total_rows,
                    "null_count": null_count,
                    "null_percent": round(null_percent, 4),
                }
            )

    df = pd.DataFrame(results)
    save_report(df, "01_null_summary.csv")

    print(df)
    return df


# ============================================================
# 5. CHECK 3: DUPLICATE KEYS
# ============================================================

def profile_duplicate_keys(engine):
    print("\n=== 3. DUPLICATE KEYS ===")

    results = []

    for table_name, keys in DUPLICATE_KEYS.items():
        key_expr = ", ".join(keys)

        query = f"""
        WITH duplicated AS (
            SELECT
                {key_expr},
                COUNT(*) AS duplicate_count
            FROM dbo.{table_name}
            GROUP BY {key_expr}
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS duplicate_groups,
            COALESCE(SUM(duplicate_count), 0) AS duplicate_rows
        FROM duplicated;
        """

        df = read_sql(engine, query)

        results.append(
            {
                "table_name": table_name,
                "key_columns": " + ".join(keys),
                "duplicate_groups": int(df["duplicate_groups"].iloc[0]),
                "duplicate_rows": int(df["duplicate_rows"].iloc[0]),
            }
        )

    result = pd.DataFrame(results)
    save_report(result, "02_duplicate_summary.csv")

    print(result)
    return result


# ============================================================
# 6. CHECK 4: ORDER STATUS DISTRIBUTION
# ============================================================

def profile_order_status(engine):
    print("\n=== 4. ORDER STATUS DISTRIBUTION ===")

    query = """
    SELECT
        order_status,
        COUNT(*) AS total_orders
    FROM dbo.stg_orders
    GROUP BY order_status
    ORDER BY total_orders DESC;
    """

    df = read_sql(engine, query)
    save_report(df, "03_order_status_distribution.csv")

    print(df)
    return df


# ============================================================
# 7. CHECK 5: MONEY QUALITY
# ============================================================

def profile_money_quality(engine):
    print("\n=== 5. MONEY QUALITY ===")

    query = """
    SELECT
        COUNT(*) AS total_rows,

        SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
        SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) AS null_freight_value,

        SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) AS invalid_price_lte_0,
        SUM(CASE WHEN freight_value < 0 THEN 1 ELSE 0 END) AS invalid_freight_lt_0,

        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price,

        MIN(freight_value) AS min_freight_value,
        MAX(freight_value) AS max_freight_value,
        AVG(freight_value) AS avg_freight_value
    FROM dbo.stg_order_items;
    """

    df = read_sql(engine, query)
    save_report(df, "04_money_quality_summary.csv")

    print(df)
    return df


# ============================================================
# 8. CHECK 6: DATE QUALITY
# ============================================================

def profile_date_quality(engine):
    print("\n=== 6. DATE QUALITY ===")

    query = """
    SELECT
        COUNT(*) AS total_orders,

        SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) AS null_purchase_date,
        SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_delivered_date,
        SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) AS null_estimated_date,

        SUM(
            CASE 
                WHEN order_delivered_customer_date < order_purchase_timestamp 
                THEN 1 ELSE 0 
            END
        ) AS delivered_before_purchase,

        SUM(
            CASE 
                WHEN order_estimated_delivery_date < order_purchase_timestamp 
                THEN 1 ELSE 0 
            END
        ) AS estimated_before_purchase,

        SUM(
            CASE 
                WHEN order_purchase_timestamp IS NOT NULL
                 AND order_delivered_customer_date IS NOT NULL
                 AND DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date) < 0
                THEN 1 ELSE 0 
            END
        ) AS negative_delivery_time,

        SUM(
            CASE 
                WHEN order_purchase_timestamp IS NOT NULL
                 AND order_delivered_customer_date IS NOT NULL
                 AND DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date) > 120
                THEN 1 ELSE 0 
            END
        ) AS very_long_delivery_time_gt_120_days
    FROM dbo.stg_orders;
    """

    df = read_sql(engine, query)
    save_report(df, "05_date_quality_summary.csv")

    print(df)
    return df


# ============================================================
# 9. CHECK 7: RELATIONSHIP / REFERENTIAL CHECK
# ============================================================

def profile_relationships(engine):
    print("\n=== 7. RELATIONSHIP CHECK ===")

    checks = {
        "order_items_without_order": """
            SELECT COUNT(*) AS issue_count
            FROM dbo.stg_order_items oi
            LEFT JOIN dbo.stg_orders o
                ON oi.order_id = o.order_id
            WHERE o.order_id IS NULL;
        """,
        "orders_without_customer": """
            SELECT COUNT(*) AS issue_count
            FROM dbo.stg_orders o
            LEFT JOIN dbo.stg_customers c
                ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL;
        """,
        "order_items_without_product": """
            SELECT COUNT(*) AS issue_count
            FROM dbo.stg_order_items oi
            LEFT JOIN dbo.stg_products p
                ON oi.product_id = p.product_id
            WHERE p.product_id IS NULL;
        """,
        "order_items_without_seller": """
            SELECT COUNT(*) AS issue_count
            FROM dbo.stg_order_items oi
            LEFT JOIN dbo.stg_sellers s
                ON oi.seller_id = s.seller_id
            WHERE s.seller_id IS NULL;
        """,
    }

    results = []

    for check_name, query in checks.items():
        df = read_sql(engine, query)
        issue_count = int(df["issue_count"].iloc[0])

        results.append(
            {
                "check_name": check_name,
                "issue_count": issue_count,
            }
        )

    result = pd.DataFrame(results)
    save_report(result, "06_relationship_summary.csv")

    print(result)
    return result


# ============================================================
# 10. CHECK 8: PRODUCT CATEGORY DISTRIBUTION
# ============================================================

def profile_product_category(engine):
    print("\n=== 8. PRODUCT CATEGORY DISTRIBUTION ===")

    query = """
    SELECT TOP 20
        product_category_name,
        COUNT(*) AS total_products
    FROM dbo.stg_products
    GROUP BY product_category_name
    ORDER BY total_products DESC;
    """

    df = read_sql(engine, query)
    save_report(df, "07_product_category_distribution.csv")

    print(df)
    return df


# ============================================================
# 11. CHECK 9: CUSTOMER / SELLER STATE DISTRIBUTION
# ============================================================

def profile_location_distribution(engine):
    print("\n=== 9. LOCATION DISTRIBUTION ===")

    customer_query = """
    SELECT
        customer_state,
        COUNT(*) AS total_customers
    FROM dbo.stg_customers
    GROUP BY customer_state
    ORDER BY total_customers DESC;
    """

    seller_query = """
    SELECT
        seller_state,
        COUNT(*) AS total_sellers
    FROM dbo.stg_sellers
    GROUP BY seller_state
    ORDER BY total_sellers DESC;
    """

    customer_df = read_sql(engine, customer_query)
    seller_df = read_sql(engine, seller_query)

    save_report(customer_df, "08_customer_state_distribution.csv")
    save_report(seller_df, "09_seller_state_distribution.csv")

    print("\nCustomer state distribution:")
    print(customer_df)

    print("\nSeller state distribution:")
    print(seller_df)

    return customer_df, seller_df


# ============================================================
# 12. SAMPLE DATA
# ============================================================

def export_sample_rows(engine):
    print("\n=== 10. EXPORT SAMPLE ROWS ===")

    for table_name in STAGING_TABLES.keys():
        query = f"""
        SELECT TOP 20 *
        FROM dbo.{table_name};
        """

        df = read_sql(engine, query)
        save_report(df, f"sample_{table_name}.csv")


# ============================================================
# 13. MAIN
# ============================================================

def main():
    print("Starting staging data profiling...")
    print(f"Output folder: {OUTPUT_DIR}")

    engine = get_engine()

    profile_table_overview(engine)
    profile_null_values(engine)
    profile_duplicate_keys(engine)
    profile_order_status(engine)
    profile_money_quality(engine)
    profile_date_quality(engine)
    profile_relationships(engine)
    profile_product_category(engine)
    profile_location_distribution(engine)
    export_sample_rows(engine)

    print("\nDONE: Staging data profiling completed.")
    print("Please check CSV files in eda/output folder.")


if __name__ == "__main__":
    main()