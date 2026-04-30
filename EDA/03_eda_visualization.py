import os
import urllib.parse

import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from sqlalchemy import create_engine


# ============================================================
# 1. CONFIG
# ============================================================

BASE_DIR = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
FIGURE_DIR = os.path.join(OUTPUT_DIR, "figures")

os.makedirs(FIGURE_DIR, exist_ok=True)


DB_CONFIG = {
    "server": "localhost",
    "port": "1433",
    "database": "olist_dw",
    "username": "pentaho_user",
    "password": "Pentaho@12345",
}


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def read_output_csv(filename: str) -> pd.DataFrame:
    path = os.path.join(OUTPUT_DIR, filename)

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing file: {path}\n"
            "Hãy chạy 01_data_profiling_staging.py và 02_data_quality_report.py trước."
        )

    return pd.read_csv(path)


def save_figure(filename: str):
    path = os.path.join(FIGURE_DIR, filename)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[OK] Saved figure: {path}")


def add_value_labels(ax):
    for container in ax.containers:
        ax.bar_label(container, fmt="%.0f", padding=3)


def get_sqlserver_driver():
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


def read_sql(engine, query: str) -> pd.DataFrame:
    return pd.read_sql_query(query, engine)


# ============================================================
# 3. CHART 1: TABLE OVERVIEW
# ============================================================

def plot_table_overview():
    df = read_output_csv("00_table_overview.csv")

    plt.figure(figsize=(10, 5))
    ax = plt.bar(df["table_name"], df["row_count"])
    plt.title("Row Count by Staging Table")
    plt.xlabel("Staging Table")
    plt.ylabel("Row Count")
    plt.xticks(rotation=30, ha="right")

    current_ax = plt.gca()
    add_value_labels(current_ax)

    save_figure("01_table_overview.png")


# ============================================================
# 4. CHART 2: MISSING VALUES
# ============================================================

def plot_missing_values():
    df = read_output_csv("01_null_summary.csv")

    missing_df = df[df["null_count"] > 0].copy()

    if missing_df.empty:
        print("[INFO] No missing values found. Skip missing values chart.")
        return

    missing_df["field"] = missing_df["table_name"] + "." + missing_df["column_name"]
    missing_df = missing_df.sort_values("null_count", ascending=True)

    plt.figure(figsize=(10, 5))
    plt.barh(missing_df["field"], missing_df["null_count"])
    plt.title("Missing Values by Important Columns")
    plt.xlabel("Null Count")
    plt.ylabel("Column")

    save_figure("02_missing_values.png")


# ============================================================
# 5. CHART 3: ORDER STATUS DISTRIBUTION
# ============================================================

def plot_order_status_distribution():
    df = read_output_csv("03_order_status_distribution.csv")

    plt.figure(figsize=(10, 5))
    plt.bar(df["order_status"], df["total_orders"])
    plt.title("Order Status Distribution")
    plt.xlabel("Order Status")
    plt.ylabel("Total Orders")
    plt.xticks(rotation=30, ha="right")

    current_ax = plt.gca()
    add_value_labels(current_ax)

    save_figure("03_order_status_distribution.png")


# ============================================================
# 6. CHART 4: PRODUCT CATEGORY DISTRIBUTION
# ============================================================

def plot_product_category_distribution():
    df = read_output_csv("07_product_category_distribution.csv")

    df["product_category_name"] = df["product_category_name"].fillna("Unknown")
    df["product_category_name"] = df["product_category_name"].replace("None", "Unknown")

    top_df = df.head(15).sort_values("total_products", ascending=True)

    plt.figure(figsize=(10, 7))
    plt.barh(top_df["product_category_name"], top_df["total_products"])
    plt.title("Top Product Categories by Number of Products")
    plt.xlabel("Total Products")
    plt.ylabel("Product Category")

    save_figure("04_top_product_categories.png")


# ============================================================
# 7. CHART 5: CUSTOMER STATE DISTRIBUTION
# ============================================================

def plot_customer_state_distribution():
    df = read_output_csv("08_customer_state_distribution.csv")

    top_df = df.head(10).sort_values("total_customers", ascending=True)

    plt.figure(figsize=(9, 5))
    plt.barh(top_df["customer_state"], top_df["total_customers"])
    plt.title("Top 10 Customer States")
    plt.xlabel("Total Customers")
    plt.ylabel("Customer State")

    save_figure("05_customer_state_distribution.png")


# ============================================================
# 8. CHART 6: SELLER STATE DISTRIBUTION
# ============================================================

def plot_seller_state_distribution():
    df = read_output_csv("09_seller_state_distribution.csv")

    top_df = df.head(10).sort_values("total_sellers", ascending=True)

    plt.figure(figsize=(9, 5))
    plt.barh(top_df["seller_state"], top_df["total_sellers"])
    plt.title("Top 10 Seller States")
    plt.xlabel("Total Sellers")
    plt.ylabel("Seller State")

    save_figure("06_seller_state_distribution.png")


# ============================================================
# 9. CHART 7: CLEANING DECISION PRIORITY
# ============================================================

def plot_cleaning_decision_priority():
    df = read_output_csv("11_cleaning_decision_matrix.csv")

    priority_df = (
        df.groupby("priority", as_index=False)
        .size()
        .rename(columns={"size": "total_rules"})
    )

    priority_order = ["High", "Medium", "Low"]
    priority_df["priority"] = pd.Categorical(
        priority_df["priority"],
        categories=priority_order,
        ordered=True
    )
    priority_df = priority_df.sort_values("priority")

    plt.figure(figsize=(7, 5))
    plt.bar(priority_df["priority"].astype(str), priority_df["total_rules"])
    plt.title("Cleaning Decisions by Priority")
    plt.xlabel("Priority")
    plt.ylabel("Number of Rules")

    current_ax = plt.gca()
    add_value_labels(current_ax)

    save_figure("07_cleaning_decision_priority.png")


# ============================================================
# 10. FACT/DASHBOARD BUSINESS CHARTS FROM DATABASE
# ============================================================

def plot_monthly_revenue(engine):
    query = """
    SELECT
        FORMAT(order_purchase_timestamp, 'yyyy-MM') AS year_month,
        SUM(total_amount) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM dbo.fact_sales
    GROUP BY FORMAT(order_purchase_timestamp, 'yyyy-MM')
    ORDER BY year_month;
    """

    df = read_sql(engine, query)

    if df.empty:
        print("[INFO] fact_sales is empty. Skip monthly revenue chart.")
        return

    plt.figure(figsize=(11, 5))
    plt.plot(df["year_month"], df["total_revenue"], marker="o")
    plt.title("Monthly Revenue")
    plt.xlabel("Month")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45, ha="right")

    save_figure("08_monthly_revenue.png")


def plot_late_order_pie(engine):
    query = """
    SELECT
        CASE 
            WHEN is_late = 1 THEN 'Late'
            ELSE 'On time or early'
        END AS delivery_status,
        COUNT(DISTINCT order_id) AS total_orders
    FROM dbo.fact_sales
    GROUP BY 
        CASE 
            WHEN is_late = 1 THEN 'Late'
            ELSE 'On time or early'
        END;
    """

    df = read_sql(engine, query)

    if df.empty:
        print("[INFO] fact_sales is empty. Skip late order pie chart.")
        return

    plt.figure(figsize=(7, 7))
    plt.pie(
        df["total_orders"],
        labels=df["delivery_status"],
        autopct="%1.1f%%",
        startangle=90
    )
    plt.title("Late vs On-time Orders")

    save_figure("09_late_order_ratio.png")


def plot_delivery_time_distribution(engine):
    query = """
    SELECT
        delivery_time_days
    FROM dbo.fact_sales
    WHERE delivery_time_days IS NOT NULL;
    """

    df = read_sql(engine, query)

    if df.empty:
        print("[INFO] fact_sales is empty. Skip delivery time distribution chart.")
        return

    plt.figure(figsize=(10, 5))
    plt.hist(df["delivery_time_days"], bins=40)
    plt.title("Delivery Time Distribution")
    plt.xlabel("Delivery Time (days)")
    plt.ylabel("Number of Order Items")

    save_figure("10_delivery_time_distribution.png")


def plot_top_category_revenue(engine):
    query = """
    SELECT TOP 10
        COALESCE(p.product_category_name, 'Unknown') AS product_category_name,
        SUM(f.total_amount) AS total_revenue
    FROM dbo.fact_sales f
    LEFT JOIN dbo.dim_product p
        ON f.product_id = p.product_id
    GROUP BY COALESCE(p.product_category_name, 'Unknown')
    ORDER BY total_revenue DESC;
    """

    df = read_sql(engine, query)

    if df.empty:
        print("[INFO] fact_sales is empty. Skip top category revenue chart.")
        return

    df = df.sort_values("total_revenue", ascending=True)

    plt.figure(figsize=(10, 6))
    plt.barh(df["product_category_name"], df["total_revenue"])
    plt.title("Top 10 Product Categories by Revenue")
    plt.xlabel("Total Revenue")
    plt.ylabel("Product Category")

    save_figure("11_top_category_revenue.png")


def plot_top_customer_state_revenue(engine):
    query = """
    SELECT TOP 10
        COALESCE(c.customer_state, 'Unknown') AS customer_state,
        SUM(f.total_amount) AS total_revenue
    FROM dbo.fact_sales f
    LEFT JOIN dbo.dim_customer c
        ON f.customer_id = c.customer_id
    GROUP BY COALESCE(c.customer_state, 'Unknown')
    ORDER BY total_revenue DESC;
    """

    df = read_sql(engine, query)

    if df.empty:
        print("[INFO] fact_sales is empty. Skip customer state revenue chart.")
        return

    df = df.sort_values("total_revenue", ascending=True)

    plt.figure(figsize=(9, 5))
    plt.barh(df["customer_state"], df["total_revenue"])
    plt.title("Top 10 Customer States by Revenue")
    plt.xlabel("Total Revenue")
    plt.ylabel("Customer State")

    save_figure("12_customer_state_revenue.png")


# ============================================================
# 11. MAIN
# ============================================================

def main():
    print("Starting EDA visualization...")
    print(f"Reading EDA outputs from: {OUTPUT_DIR}")
    print(f"Saving figures to: {FIGURE_DIR}")

    # Charts from CSV profiling outputs
    plot_table_overview()
    plot_missing_values()
    plot_order_status_distribution()
    plot_product_category_distribution()
    plot_customer_state_distribution()
    plot_seller_state_distribution()
    plot_cleaning_decision_priority()

    # Charts from fact/dim tables
    try:
        engine = get_engine()

        plot_monthly_revenue(engine)
        plot_late_order_pie(engine)
        plot_delivery_time_distribution(engine)
        plot_top_category_revenue(engine)
        plot_top_customer_state_revenue(engine)

    except Exception as e:
        print("[WARNING] Cannot generate DB-based charts.")
        print("Reason:", e)
        print("Staging/profiling charts were still generated successfully.")

    print("\nDONE: EDA visualization completed.")
    print("Please check PNG files in EDA/output/figures.")


if __name__ == "__main__":
    main()