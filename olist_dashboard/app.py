import urllib.parse

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

# ─────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────
st.set_page_config(
    page_title="Olist – Báo cáo Kinh doanh",
    page_icon="📈",
    layout="wide",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

/* ── Nền tổng thể ── */
html, body { font-family: 'IBM Plex Sans', sans-serif; }
.stApp {
    background-color: #F7F8FA !important;
    font-family: 'IBM Plex Sans', sans-serif;
    color: #1A1F2E;
}
.block-container {
    padding: 24px 28px !important;
    max-width: 100% !important;
    background-color: #F7F8FA !important;
}

/* ── Main content text màu tối ── */
.stApp p, .stApp span, .stApp label,
.stApp h1, .stApp h2, .stApp h3 {
    color: #1A1F2E !important;
}

/* ── Tabs: text rõ, không tràn emoji ── */
[data-testid="stTabs"] button {
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    color: #475569 !important;
    padding: 8px 16px !important;
    white-space: nowrap !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #1D4ED8 !important;
    font-weight: 600 !important;
}
[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
    background-color: #1D4ED8 !important;
}
[data-testid="stTabs"] [data-baseweb="tab-border"] {
    background-color: #E2E6EF !important;
}

/* ── Expander: text và nền rõ ── */
[data-testid="stExpander"] {
    background-color: #FFFFFF !important;
    border: 1px solid #E2E6EF !important;
    border-radius: 6px !important;
}
[data-testid="stExpander"] summary {
    background-color: #FFFFFF !important;
    color: #1A1F2E !important;
    font-weight: 500 !important;
    font-size: 14px !important;
    padding: 12px 16px !important;
}
[data-testid="stExpander"] summary:hover {
    background-color: #F8FAFC !important;
}
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span {
    color: #1A1F2E !important;
}
[data-testid="stExpander"] svg {
    fill: #475569 !important;
    color: #475569 !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div {
    background-color: #FFFFFF !important;
    border-right: 1px solid #E2E6EF !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: #1A1F2E !important;
    background-color: transparent !important;
}

/* Sidebar: ô multiselect */
[data-testid="stSidebar"] [data-baseweb="select"] > div:first-child {
    background-color: #F8FAFC !important;
    border: 1px solid #CBD5E1 !important;
    border-radius: 6px !important;
}

/* Sidebar: tag được chọn — xanh nhạt, chữ xanh đậm */
[data-testid="stSidebar"] [data-baseweb="tag"] {
    background-color: #DBEAFE !important;
    border: 1px solid #93C5FD !important;
    border-radius: 4px !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] span {
    color: #1E40AF !important;
    font-size: 12px !important;
    font-weight: 500 !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] svg { fill: #1E40AF !important; }

/* Sidebar: date input */
[data-testid="stSidebar"] input {
    background-color: #F8FAFC !important;
    border: 1px solid #CBD5E1 !important;
    color: #0F172A !important;
    border-radius: 6px !important;
}
[data-testid="stSidebar"] [data-testid="stDateInputPresentationStyledDiv"] {
    background-color: #F8FAFC !important;
    border: 1px solid #CBD5E1 !important;
    border-radius: 6px !important;
}

/* Sidebar: caption/small text */
[data-testid="stSidebar"] .stCaption,
[data-testid="stSidebar"] small {
    color: #64748B !important;
}

/* Dropdown khi mở (toàn trang) */
[data-baseweb="popover"] [data-baseweb="menu"],
[data-baseweb="popover"] ul {
    background-color: #FFFFFF !important;
}
[data-baseweb="popover"] li {
    background-color: #FFFFFF !important;
    color: #1A1F2E !important;
    font-size: 13px !important;
}
[data-baseweb="popover"] li:hover {
    background-color: #EFF6FF !important;
    color: #1D4ED8 !important;
}

/* ── KPI cards ── */
.kpi-wrap {
    background: #FFFFFF;
    border: 1px solid #E2E6EF;
    border-radius: 6px;
    padding: 20px 22px;
}
.kpi-wrap.accent-blue  { border-top: 3px solid #1D4ED8; }
.kpi-wrap.accent-green { border-top: 3px solid #059669; }
.kpi-wrap.accent-amber { border-top: 3px solid #D97706; }
.kpi-wrap.accent-slate { border-top: 3px solid #475569; }
.kpi-wrap.accent-red   { border-top: 3px solid #DC2626; }
.kpi-label {
    font-size: 11px; font-weight: 600;
    letter-spacing: 0.8px; text-transform: uppercase;
    color: #94A3B8; margin-bottom: 8px;
}
.kpi-number {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 26px; font-weight: 500;
    color: #0F172A; line-height: 1; margin-bottom: 4px;
}
.kpi-sub { font-size: 12px; color: #94A3B8; }

/* ── Page header ── */
.page-header {
    background: #FFFFFF;
    border: 1px solid #E2E6EF;
    border-left: 4px solid #1D4ED8;
    border-radius: 6px;
    padding: 20px 28px; margin-bottom: 24px;
}
.page-header h1 {
    font-size: 22px; font-weight: 700;
    color: #0F172A; margin: 0 0 4px 0; letter-spacing: -0.3px;
}
.page-header p { font-size: 13px; color: #64748B; margin: 0; }

/* ── Section label ── */
.section-label {
    font-size: 11px; font-weight: 600;
    letter-spacing: 1.2px; text-transform: uppercase;
    color: #94A3B8; margin: 28px 0 12px 0;
    padding-bottom: 8px; border-bottom: 1px solid #E2E6EF;
}

/* ── Insight box ── */
.insight-box {
    background: #EFF6FF; border: 1px solid #BFDBFE;
    border-radius: 6px; padding: 14px 18px;
    font-size: 13px; color: #1E40AF;
    margin-bottom: 6px; line-height: 1.6;
}

/* ── Chart containers ── */
.element-container:has(.stPlotlyChart) {
    background: #FFFFFF;
    border: 1px solid #E2E6EF;
    border-radius: 6px;
    padding: 2px;
}

/* ── Ẩn metric mặc định ── */
[data-testid="stMetric"] { display: none; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# PLOTLY LAYOUT HELPER  — không có 'title' trong base
# ─────────────────────────────────────────
def chart_layout(**kwargs):
    base = dict(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(family="IBM Plex Sans, sans-serif", color="#475569", size=12),
        margin=dict(l=16, r=16, t=52, b=16),
        xaxis=dict(
            showgrid=True, gridcolor="#F1F5F9", gridwidth=1,
            zeroline=False, tickfont=dict(size=11, color="#64748B"),
            linecolor="#E2E6EF", showline=True,
        ),
        yaxis=dict(
            showgrid=True, gridcolor="#F1F5F9", gridwidth=1,
            zeroline=False, tickfont=dict(size=11, color="#64748B"),
            linecolor="#E2E6EF", showline=False,
        ),
        hoverlabel=dict(
            bgcolor="#0F172A", bordercolor="#0F172A",
            font=dict(family="IBM Plex Sans", color="#F8FAFC", size=12),
        ),
        legend=dict(
            bgcolor="#FFFFFF", bordercolor="#E2E6EF", borderwidth=1,
            font=dict(size=11),
        ),
    )
    base.update(kwargs)   # title được truyền qua đây, không bị duplicate
    return base


BLUE   = "#1D4ED8"
BLUE_L = "#93C5FD"
GREEN  = "#059669"
AMBER  = "#D97706"
RED    = "#DC2626"
SLATE  = "#475569"


# ─────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────
@st.cache_resource
def get_engine():
    server, database = "localhost", "olist_dw"
    username, password = "pentaho_user", "Pentaho@12345"
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server},1433;DATABASE={database};"
        f"UID={username};PWD={password};TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}")


@st.cache_data
def load_data():
    df = pd.read_sql("""
        SELECT
            f.order_id, f.customer_id, f.seller_id, f.product_id,
            f.price, f.freight_value, f.total_amount,
            f.order_purchase_timestamp,
            f.delivery_time_days, f.delivery_delay_days, f.is_late,
            c.customer_state,
            p.product_category_name,
            s.seller_state
        FROM dbo.fact_sales f
        LEFT JOIN dbo.dim_customer c ON f.customer_id = c.customer_id
        LEFT JOIN dbo.dim_product  p ON f.product_id  = p.product_id
        LEFT JOIN dbo.dim_seller   s ON f.seller_id   = s.seller_id
    """, get_engine())

    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["year_month"] = df["order_purchase_timestamp"].dt.to_period("M").astype(str)
    df["month_dt"]   = df["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp()
    df["product_category_name"] = df["product_category_name"].fillna("Không rõ")
    df["customer_state"]        = df["customer_state"].fillna("Không rõ")
    df["seller_state"]          = df["seller_state"].fillna("Không rõ")
    return df


# ─────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────
try:
    df = load_data()
except Exception as e:
    st.error("Không thể kết nối cơ sở dữ liệu.")
    st.exception(e)
    st.stop()


# ─────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔎 Bộ lọc dữ liệu")
    st.caption("Điều chỉnh các bộ lọc bên dưới để thu hẹp phạm vi phân tích.")
    st.markdown("---")

    # ── Thời gian ──
    min_d = df["order_purchase_timestamp"].min().date()
    max_d = df["order_purchase_timestamp"].max().date()
    st.markdown("**📅 Khoảng thời gian đặt hàng**")
    st.caption("Chọn từ ngày — đến ngày cần xem.")
    date_range = st.date_input(
        "Thời gian",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d,
        label_visibility="collapsed",
    )

    st.markdown("---")

    # ── Bang khách hàng ──
    all_states = sorted(df["customer_state"].unique())
    st.markdown("**🗺️ Bang của khách hàng**")
    st.caption(f"Lọc theo địa lý. Hiện có {len(all_states)} bang.")
    sel_states = st.multiselect(
        "Bang khách hàng",
        options=all_states,
        default=all_states,
        label_visibility="collapsed",
        placeholder="Chọn bang...",
    )

    st.markdown("---")

    # ── Danh mục ──
    all_cats = sorted(df["product_category_name"].unique())
    st.markdown("**📦 Danh mục sản phẩm**")
    st.caption(f"Lọc theo nhóm hàng. Hiện có {len(all_cats)} danh mục.")
    sel_cats = st.multiselect(
        "Danh mục",
        options=all_cats,
        default=all_cats,
        label_visibility="collapsed",
        placeholder="Chọn danh mục...",
    )

    st.markdown("---")
    st.caption(f"Tổng bản ghi gốc: **{len(df):,}**")


# ─────────────────────────────────────────
# FILTER
# ─────────────────────────────────────────
fdf = df.copy()
if len(date_range) == 2:
    fdf = fdf[
        (fdf["order_purchase_timestamp"].dt.date >= date_range[0]) &
        (fdf["order_purchase_timestamp"].dt.date <= date_range[1])
    ]
fdf = fdf[
    fdf["customer_state"].isin(sel_states) &
    fdf["product_category_name"].isin(sel_cats)
]


# ─────────────────────────────────────────
# KPI CALCULATIONS
# ─────────────────────────────────────────
revenue      = fdf["total_amount"].sum()
n_orders     = fdf["order_id"].nunique()
avg_order    = revenue / n_orders if n_orders else 0
avg_delivery = fdf["delivery_time_days"].mean()
late_pct     = fdf["is_late"].mean() * 100 if len(fdf) else 0
freight_pct  = fdf["freight_value"].sum() / revenue * 100 if revenue else 0


# ─────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────
d0 = date_range[0] if len(date_range) == 2 else "—"
d1 = date_range[1] if len(date_range) == 2 else "—"
st.markdown(f"""
<div class="page-header">
    <h1>📈 Báo cáo Kinh doanh – Olist E-Commerce</h1>
    <p>Dữ liệu lọc: <strong>{len(fdf):,}</strong> giao dịch &nbsp;·&nbsp;
       Từ <strong>{d0}</strong> đến <strong>{d1}</strong>
    </p>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# KPI CARDS
# ─────────────────────────────────────────
st.markdown('<div class="section-label">Chỉ số kinh doanh tổng hợp</div>',
            unsafe_allow_html=True)

k1, k2, k3, k4, k5 = st.columns(5)
kpis = [
    (k1, "accent-blue",  "Tổng Doanh Thu",   f"R$ {revenue:,.0f}",     "Giá trị toàn bộ đơn hàng"),
    (k2, "accent-green", "Số Đơn Hàng",      f"{n_orders:,}",          "Đơn hàng duy nhất"),
    (k3, "accent-slate", "Giá Trị TB / Đơn", f"R$ {avg_order:,.0f}",  "Doanh thu trung bình mỗi đơn"),
    (k4, "accent-amber", "TG Giao Hàng TB",  f"{avg_delivery:.1f} ng","Ngày từ đặt đến nhận"),
    (k5, "accent-red",   "Tỷ Lệ Giao Trễ",  f"{late_pct:.1f}%",      f"Cước vận chuyển: {freight_pct:.1f}% DT"),
]
for col, cls, lbl, val, sub in kpis:
    with col:
        st.markdown(f"""
        <div class="kpi-wrap {cls}">
            <div class="kpi-label">{lbl}</div>
            <div class="kpi-number">{val}</div>
            <div class="kpi-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# TABS
# ─────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "Xu Hướng Theo Thời Gian",
    "Địa Lý & Danh Mục",
    "Hiệu Suất Giao Hàng",
])


# ══════════════════════════════════════════
# TAB 1 – XU HƯỚNG
# ══════════════════════════════════════════
with tab1:
    monthly = (
        fdf.groupby(["year_month", "month_dt"], as_index=False)
           .agg(
               revenue=("total_amount", "sum"),
               orders=("order_id", "nunique"),
               avg_val=("total_amount", "mean"),
           )
           .sort_values("month_dt")
    )

    # Doanh thu + trung bình 3 tháng
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=monthly["year_month"], y=monthly["revenue"],
        name="Doanh thu tháng",
        marker_color=BLUE_L,
        hovertemplate="<b>%{x}</b><br>Doanh thu: R$ %{y:,.0f}<extra></extra>",
    ))
    fig1.add_trace(go.Scatter(
        x=monthly["year_month"],
        y=monthly["revenue"].rolling(3, min_periods=1).mean(),
        name="Trung bình 3 tháng",
        mode="lines",
        line=dict(color=BLUE, width=2, dash="dot"),
        hovertemplate="<b>%{x}</b><br>TB 3 tháng: R$ %{y:,.0f}<extra></extra>",
    ))
    fig1.update_layout(chart_layout(
        title="Doanh thu theo tháng — kèm xu hướng trung bình động 3 tháng",
        yaxis_title="Doanh thu (R$)",
        legend=dict(
            orientation="h", yanchor="bottom", y=1.01,
            xanchor="right", x=1,
        ),
        height=320,
    ))
    st.plotly_chart(fig1, use_container_width=True)

    col_a, col_b = st.columns(2)

    # Số đơn theo tháng
    fig2 = go.Figure(go.Scatter(
        x=monthly["year_month"], y=monthly["orders"],
        mode="lines+markers",
        line=dict(color=GREEN, width=2),
        marker=dict(size=5, color=GREEN),
        fill="tozeroy", fillcolor="rgba(5,150,105,0.07)",
        hovertemplate="<b>%{x}</b><br>Đơn hàng: %{y:,}<extra></extra>",
    ))
    fig2.update_layout(chart_layout(
        title="Số đơn hàng theo tháng",
        yaxis_title="Số đơn",
        showlegend=False,
        height=280,
    ))
    col_a.plotly_chart(fig2, use_container_width=True)

    # Giá trị TB đơn
    fig3 = go.Figure(go.Scatter(
        x=monthly["year_month"], y=monthly["avg_val"],
        mode="lines+markers",
        line=dict(color=AMBER, width=2),
        marker=dict(size=5, color=AMBER),
        hovertemplate="<b>%{x}</b><br>Giá trị TB: R$ %{y:,.0f}<extra></extra>",
    ))
    fig3.update_layout(chart_layout(
        title="Giá trị trung bình mỗi đơn hàng",
        yaxis_title="R$",
        showlegend=False,
        height=280,
    ))
    col_b.plotly_chart(fig3, use_container_width=True)

    best = monthly.loc[monthly["revenue"].idxmax()]
    st.markdown(f"""
    <div class="insight-box">
        💡 <strong>Nhận xét:</strong> Tháng đạt doanh thu cao nhất là
        <strong>{best['year_month']}</strong> với
        <strong>R$ {best['revenue']:,.0f}</strong>.
        Đường trung bình động 3 tháng giúp loại bỏ biến động ngắn hạn và
        phản ánh xu hướng tăng trưởng thực sự của doanh nghiệp.
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════
# TAB 2 – ĐỊA LÝ & DANH MỤC
# ══════════════════════════════════════════
with tab2:
    col_c, col_d = st.columns(2)

    state_rev = (
        fdf.groupby("customer_state", as_index=False)
           .agg(revenue=("total_amount", "sum"), orders=("order_id", "nunique"))
           .sort_values("revenue", ascending=False)
           .head(12)
    )
    state_rev["pct"] = state_rev["revenue"] / state_rev["revenue"].sum() * 100

    fig4 = go.Figure(go.Bar(
        x=state_rev["revenue"],
        y=state_rev["customer_state"],
        orientation="h",
        marker=dict(
            color=state_rev["revenue"],
            colorscale=[[0, "#DBEAFE"], [1, "#1D4ED8"]],
            showscale=False,
        ),
        text=state_rev["pct"].map("{:.1f}%".format),
        textposition="outside",
        textfont=dict(size=11, color="#64748B"),
        hovertemplate="<b>Bang %{y}</b><br>Doanh thu: R$ %{x:,.0f}<extra></extra>",
    ))
    fig4.update_layout(chart_layout(
        title="Doanh thu theo bang khách hàng (Top 12)",
        xaxis_title="Doanh thu (R$)",
        yaxis=dict(
            categoryorder="total ascending",
            showgrid=False,
            tickfont=dict(size=11, color="#475569"),
        ),
        height=400,
    ))
    col_c.plotly_chart(fig4, use_container_width=True)

    cat_rev = (
        fdf.groupby("product_category_name", as_index=False)
           .agg(revenue=("total_amount", "sum"), orders=("order_id", "nunique"))
           .sort_values("revenue", ascending=False)
           .head(12)
    )

    fig5 = go.Figure(go.Bar(
        x=cat_rev["revenue"],
        y=cat_rev["product_category_name"],
        orientation="h",
        marker=dict(
            color=cat_rev["revenue"],
            colorscale=[[0, "#ECFDF5"], [1, "#059669"]],
            showscale=False,
        ),
        hovertemplate="<b>%{y}</b><br>Doanh thu: R$ %{x:,.0f}<extra></extra>",
    ))
    fig5.update_layout(chart_layout(
        title="Doanh thu theo danh mục sản phẩm (Top 12)",
        xaxis_title="Doanh thu (R$)",
        yaxis=dict(
            categoryorder="total ascending",
            showgrid=False,
            tickfont=dict(size=11, color="#475569"),
        ),
        height=400,
    ))
    col_d.plotly_chart(fig5, use_container_width=True)

    # Heatmap bang × danh mục
    st.markdown('<div class="section-label">Phân tích chéo: Bang × Danh mục (Top 8 × Top 8)</div>',
                unsafe_allow_html=True)

    top8_states = state_rev["customer_state"].head(8).tolist()
    top8_cats   = cat_rev["product_category_name"].head(8).tolist()

    heat_df = (
        fdf[
            fdf["customer_state"].isin(top8_states) &
            fdf["product_category_name"].isin(top8_cats)
        ]
        .groupby(["customer_state", "product_category_name"], as_index=False)
        .agg(revenue=("total_amount", "sum"))
        .pivot(index="customer_state", columns="product_category_name", values="revenue")
        .fillna(0)
    )

    fig6 = go.Figure(go.Heatmap(
        z=heat_df.values,
        x=heat_df.columns.tolist(),
        y=heat_df.index.tolist(),
        colorscale=[[0, "#F0F9FF"], [0.5, "#60A5FA"], [1, "#1D4ED8"]],
        hovertemplate=(
            "Bang: <b>%{y}</b><br>"
            "Danh mục: <b>%{x}</b><br>"
            "Doanh thu: R$ %{z:,.0f}<extra></extra>"
        ),
        showscale=True,
        colorbar=dict(
            tickfont=dict(size=10),
            title=dict(text="R$", side="right"),
        ),
    ))
    fig6.update_layout(chart_layout(
        title="Bản đồ nhiệt: Doanh thu theo Bang × Danh mục",
        xaxis=dict(tickangle=-35, tickfont=dict(size=10), showgrid=False),
        yaxis=dict(tickfont=dict(size=11), showgrid=False),
        height=340,
    ))
    st.plotly_chart(fig6, use_container_width=True)


# ══════════════════════════════════════════
# TAB 3 – GIAO HÀNG
# ══════════════════════════════════════════
with tab3:
    col_e, col_f = st.columns([1.4, 1])

    del_clean = fdf["delivery_time_days"].dropna()

    # Histogram phân phối
    fig7 = go.Figure()
    fig7.add_trace(go.Histogram(
        x=del_clean, nbinsx=35,
        marker_color=BLUE_L,
        marker_line_color=BLUE,
        marker_line_width=0.5,
        hovertemplate="Thời gian %{x} ngày: %{y:,} đơn<extra></extra>",
    ))
    fig7.add_vline(
        x=del_clean.mean(), line_width=2, line_dash="dash",
        line_color=BLUE,
        annotation_text=f"TB: {del_clean.mean():.1f} ngày",
        annotation_position="top right",
        annotation_font=dict(color=BLUE, size=12),
    )
    fig7.add_vline(
        x=del_clean.median(), line_width=1.5, line_dash="dot",
        line_color=SLATE,
        annotation_text=f"Trung vị: {del_clean.median():.1f} ngày",
        annotation_position="top left",
        annotation_font=dict(color=SLATE, size=11),
    )
    fig7.update_layout(chart_layout(
        title="Phân phối thời gian giao hàng",
        xaxis_title="Số ngày giao",
        yaxis_title="Số đơn hàng",
        showlegend=False,
        height=320,
    ))
    col_e.plotly_chart(fig7, use_container_width=True)

    # Donut giao trễ
    late_df = (
        fdf.assign(status=fdf["is_late"].map({1: "Giao trễ", 0: "Đúng hẹn"}))
           .groupby("status", as_index=False)
           .agg(n=("order_id", "nunique"))
    )
    fig8 = go.Figure(go.Pie(
        labels=late_df["status"],
        values=late_df["n"],
        hole=0.55,
        marker=dict(
            colors=[RED, GREEN],
            line=dict(color="#FFFFFF", width=2),
        ),
        textfont=dict(size=13),
        hovertemplate="<b>%{label}</b>: %{value:,} đơn (%{percent})<extra></extra>",
    ))
    fig8.update_layout(chart_layout(
        title="Tỷ lệ giao hàng đúng hẹn",
        showlegend=True,
        height=320,
        legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.05),
        annotations=[dict(
            text=f"{late_pct:.1f}%<br><span style='font-size:11px'>trễ</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20, color=RED, family="IBM Plex Mono"),
        )],
    ))
    col_f.plotly_chart(fig8, use_container_width=True)

    # Tỷ lệ trễ theo bang
    state_late = (
        fdf.groupby("customer_state")
           .agg(late_rate=("is_late", "mean"), orders=("order_id", "nunique"))
           .reset_index()
           .query("orders >= 50")
           .sort_values("late_rate", ascending=False)
           .head(15)
    )
    state_late["late_pct_val"] = state_late["late_rate"] * 100

    fig9 = go.Figure(go.Bar(
        x=state_late["customer_state"],
        y=state_late["late_pct_val"],
        marker=dict(
            color=state_late["late_pct_val"],
            colorscale=[[0, "#FEF3C7"], [0.5, "#F59E0B"], [1, "#DC2626"]],
            showscale=True,
            colorbar=dict(title=dict(text="%"), tickfont=dict(size=10)),
        ),
        hovertemplate="<b>Bang %{x}</b><br>Tỷ lệ trễ: %{y:.1f}%<extra></extra>",
    ))
    fig9.add_hline(
        y=late_pct, line_dash="dash", line_color=SLATE, line_width=1.5,
        annotation_text=f"TB toàn quốc: {late_pct:.1f}%",
        annotation_font=dict(size=11, color=SLATE),
    )
    fig9.update_layout(chart_layout(
        title="Tỷ lệ giao trễ theo bang (bang có ≥ 50 đơn)",
        xaxis_title="Bang",
        yaxis_title="Tỷ lệ trễ (%)",
        showlegend=False,
        height=320,
    ))
    st.plotly_chart(fig9, use_container_width=True)

    col_g, col_h = st.columns(2)
    col_g.markdown(f"""
    <div class="insight-box">
        💡 <strong>Giao hàng:</strong> Trung bình <strong>{del_clean.mean():.1f} ngày</strong>,
        trung vị <strong>{del_clean.median():.1f} ngày</strong>.
        Tỷ lệ trễ <strong>{late_pct:.1f}%</strong> — cần theo dõi đặc biệt các bang
        vượt mức trung bình toàn quốc.
    </div>
    """, unsafe_allow_html=True)
    col_h.markdown(f"""
    <div class="insight-box">
        💡 <strong>Chi phí vận chuyển:</strong> Chiếm <strong>{freight_pct:.1f}%</strong>
        tổng doanh thu. Mức lý tưởng trong thương mại điện tử thường dưới 10% —
        cần tối ưu logistics nếu chỉ số này cao hơn ngưỡng đó.
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────
# BẢNG CHI TIẾT
# ─────────────────────────────────────────
with st.expander("Xem dữ liệu chi tiết — 500 dòng đầu", expanded=False):
    show = fdf[[
        "order_id", "customer_state", "product_category_name", "seller_state",
        "price", "freight_value", "total_amount",
        "delivery_time_days", "delivery_delay_days", "is_late",
        "order_purchase_timestamp",
    ]].head(500).copy()
    show["is_late"] = show["is_late"].map({1: "⚠️ Trễ", 0: "✅ Đúng hẹn"})

    st.dataframe(
        show,
        use_container_width=True,
        height=380,
        column_config={
            "order_id":                 st.column_config.TextColumn("Mã đơn"),
            "customer_state":           st.column_config.TextColumn("Bang KH"),
            "seller_state":             st.column_config.TextColumn("Bang Seller"),
            "product_category_name":    st.column_config.TextColumn("Danh mục"),
            "price":                    st.column_config.NumberColumn("Giá (R$)", format="R$ %.2f"),
            "freight_value":            st.column_config.NumberColumn("Cước (R$)", format="R$ %.2f"),
            "total_amount":             st.column_config.NumberColumn("Tổng (R$)", format="R$ %.2f"),
            "delivery_time_days":       st.column_config.ProgressColumn(
                "Ngày giao", min_value=0, max_value=60, format="%d ng"
            ),
            "delivery_delay_days":      st.column_config.NumberColumn("Trễ (ng)"),
            "is_late":                  st.column_config.TextColumn("Trạng thái"),
            "order_purchase_timestamp": st.column_config.DatetimeColumn(
                "Ngày đặt", format="DD/MM/YYYY"
            ),
        },
    )

st.markdown("""
<div style="text-align:center; padding:28px 0 8px; font-size:11px;
            color:#CBD5E1; letter-spacing:0.8px;">
    Olist Sales Intelligence &nbsp;·&nbsp; Data Warehouse Analytics Platform
</div>
""", unsafe_allow_html=True)