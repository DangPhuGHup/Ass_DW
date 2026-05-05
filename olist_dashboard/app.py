# -*- coding: utf-8 -*-
import urllib.parse

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Olist - Bao cao Kinh doanh", layout="wide")

# ──────────────────────────────────────────────
# CSS — chu DEN tren nen SANG, chu TRANG tren nen TOI
# ──────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

html, body { font-family: 'IBM Plex Sans', sans-serif; background: #F7F8FA; }
.stApp     { background: #F7F8FA !important; font-family: 'IBM Plex Sans', sans-serif; }
.block-container { padding: 24px 28px !important; max-width: 100% !important; }

/* --- Moi text tren nen sang = DEN --- */
.stApp p, .stApp span, .stApp label,
.stApp h1, .stApp h2, .stApp h3 { color: #111827 !important; }

/* Tabs */
[data-testid="stTabs"] button {
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-size: 13px !important; font-weight: 500 !important;
    color: #111827 !important; padding: 8px 20px !important;
    white-space: nowrap !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #1D4ED8 !important; font-weight: 700 !important;
}
[data-testid="stTabs"] [data-baseweb="tab-highlight"] { background-color: #1D4ED8 !important; }
[data-testid="stTabs"] [data-baseweb="tab-border"]    { background-color: #D1D5DB !important; }

/* Expander */
[data-testid="stExpander"] {
    background: #FFFFFF !important;
    border: 1px solid #D1D5DB !important; border-radius: 6px !important;
}
[data-testid="stExpander"] summary { background: #FFFFFF !important; padding: 12px 16px !important; }
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span,
[data-testid="stExpander"] summary div { color: #111827 !important; font-size: 14px !important; font-weight: 500 !important; }
[data-testid="stExpander"] svg { fill: #374151 !important; }

/* Sidebar nen trang, chu den */
[data-testid="stSidebar"], [data-testid="stSidebar"] > div {
    background: #FFFFFF !important; border-right: 1px solid #E2E6EF !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] div,
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 { color: #111827 !important; background: transparent !important; }
[data-testid="stSidebar"] .stCaption p { color: #6B7280 !important; }

/* Sidebar multiselect */
[data-testid="stSidebar"] [data-baseweb="select"] > div:first-child {
    background: #F9FAFB !important; border: 1px solid #D1D5DB !important; border-radius: 6px !important;
}
/* Tag: nen xanh, chu xanh dam — LUON doc duoc */
[data-testid="stSidebar"] [data-baseweb="tag"] {
    background: #DBEAFE !important; border: 1px solid #93C5FD !important; border-radius: 4px !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] span { color: #1E3A8A !important; font-weight: 600 !important; font-size: 12px !important; }
[data-testid="stSidebar"] [data-baseweb="tag"] svg  { fill: #1E3A8A !important; }

/* Sidebar date input */
[data-testid="stSidebar"] input {
    background: #F9FAFB !important; border: 1px solid #D1D5DB !important;
    color: #111827 !important; border-radius: 6px !important;
}
[data-testid="stSidebar"] [data-testid="stDateInputPresentationStyledDiv"] {
    background: #F9FAFB !important; border: 1px solid #D1D5DB !important; border-radius: 6px !important;
}

/* Dropdown list */
[data-baseweb="popover"] [data-baseweb="menu"], [data-baseweb="popover"] ul { background: #FFFFFF !important; }
[data-baseweb="popover"] li { background: #FFFFFF !important; color: #111827 !important; font-size: 13px !important; }
[data-baseweb="popover"] li:hover { background: #EFF6FF !important; color: #1D4ED8 !important; }

/* KPI cards */
.kpi-wrap { background: #FFFFFF; border: 1px solid #E2E6EF; border-radius: 6px; padding: 20px 22px; }
.kpi-wrap.c-blue  { border-top: 3px solid #1D4ED8; }
.kpi-wrap.c-green { border-top: 3px solid #059669; }
.kpi-wrap.c-amber { border-top: 3px solid #D97706; }
.kpi-wrap.c-slate { border-top: 3px solid #475569; }
.kpi-wrap.c-red   { border-top: 3px solid #DC2626; }
.kpi-label  { font-size: 11px; font-weight: 600; letter-spacing: 0.8px; text-transform: uppercase; color: #6B7280 !important; margin-bottom: 8px; }
.kpi-number { font-family: 'IBM Plex Mono', monospace; font-size: 26px; font-weight: 500; color: #111827 !important; line-height: 1; margin-bottom: 4px; }
.kpi-sub    { font-size: 12px; color: #6B7280 !important; }

/* Page header */
.page-header { background: #FFFFFF; border: 1px solid #E2E6EF; border-left: 4px solid #1D4ED8; border-radius: 6px; padding: 20px 28px; margin-bottom: 24px; }
.page-header h1 { font-size: 22px; font-weight: 700; color: #111827 !important; margin: 0 0 4px 0; }
.page-header p  { font-size: 13px; color: #6B7280 !important; margin: 0; }

/* Section label */
.section-label { font-size: 11px; font-weight: 600; letter-spacing: 1.2px; text-transform: uppercase; color: #6B7280 !important; margin: 28px 0 12px 0; padding-bottom: 8px; border-bottom: 1px solid #E2E6EF; }

/* Insight box: nen xanh nhat, chu xanh dam — doc duoc */
.insight-box { background: #EFF6FF; border: 1px solid #BFDBFE; border-radius: 6px; padding: 14px 18px; font-size: 13px; color: #1E3A8A !important; margin-bottom: 6px; line-height: 1.6; }

/* Chart wrapper */
.element-container:has(.stPlotlyChart) { background: #FFFFFF; border: 1px solid #E2E6EF; border-radius: 6px; padding: 2px; }

[data-testid="stMetric"] { display: none; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# PLOTLY: ep mau DEN tuyet doi (#000000) tren nen trang
# ──────────────────────────────────────────────
BLACK       = "#000000"   # Ép màu đen tuyệt đối cho text
DARK_GRAY   = "#111827"   # Dành cho các thành phần phụ
MED_GRAY    = "#374151"
GRID        = "#E5E7EB"   # Lưới đậm hơn một chút để dễ phân biệt
BORDER      = "#9CA3AF"
WHITE       = "#FFFFFF"

TITLE_FONT  = dict(family="IBM Plex Sans, sans-serif", size=14, color=BLACK)
AXIS_FONT   = dict(family="IBM Plex Sans, sans-serif", size=11, color=BLACK)
TICK_FONT   = dict(family="IBM Plex Sans, sans-serif", size=11, color=BLACK)
LEGEND_FONT = dict(family="IBM Plex Sans, sans-serif", size=11, color=BLACK)

BLUE   = "#1D4ED8"
BLUE_L = "#93C5FD"
GREEN  = "#059669"
AMBER  = "#D97706"
RED    = "#DC2626"
SLATE  = "#374151"


def base_axis():
    return dict(
        showgrid=True,
        gridcolor=GRID,
        gridwidth=0.5,
        zeroline=False,
        linecolor=BORDER,
        showline=True,
        tickfont=TICK_FONT,
        title_font=AXIS_FONT,   # tiêu đề trục luôn dùng màu BLACK
        tickcolor=BORDER,
    )


def chart_layout(title_text, xaxis_extra=None, yaxis_extra=None, **kwargs):
    xax = base_axis()
    yax = base_axis()

    if xaxis_extra:
        xax.update(xaxis_extra)
    if yaxis_extra:
        yax.update(yaxis_extra)

    # Plotly chi nhan mau tu title=dict(...), KHONG nhan tu title_font khi title la string.
    # Ep cung: neu title la string thi chuyen thanh dict voi font DEN.
    for ax in (xax, yax):
        if "title" in ax:
            if isinstance(ax["title"], str):
                ax["title"] = dict(text=ax["title"], font=AXIS_FONT)
            elif isinstance(ax["title"], dict) and "font" not in ax["title"]:
                ax["title"]["font"] = AXIS_FONT
        # tickfont va title_font luon den
        ax["tickfont"]   = TICK_FONT
        ax["title_font"] = AXIS_FONT

    layout = dict(
        paper_bgcolor=WHITE,
        plot_bgcolor=WHITE,
        font=dict(family="IBM Plex Sans, sans-serif", color=BLACK, size=12),
        title=dict(text=f"<b>{title_text}</b>", font=TITLE_FONT, x=0.0, xanchor="left"),
        margin=dict(l=50, r=20, t=60, b=50),
        xaxis=xax,
        yaxis=yax,
        hoverlabel=dict(bgcolor=BLACK, bordercolor=BLACK,
                        font=dict(family="IBM Plex Sans", color=WHITE, size=12)),
        legend=dict(bgcolor=WHITE, bordercolor=BORDER, borderwidth=1, font=LEGEND_FONT),
    )
    layout.update(kwargs)
    return layout


# ──────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────
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


try:
    df = load_data()
except Exception as e:
    st.error("Không thể kết nối cơ sở dữ liệu.")
    st.exception(e)
    st.stop()


# ──────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Bộ lọc dữ liệu")
    st.caption("Điều chỉnh bộ lọc để thu hẹp phạm vi phân tích.")
    st.markdown("---")

    min_d = df["order_purchase_timestamp"].min().date()
    max_d = df["order_purchase_timestamp"].max().date()
    st.markdown("**Khoảng thời gian đặt hàng**")
    st.caption("Chọn từ ngày — đến ngày cần xem.")
    date_range = st.date_input(
        "Thời gian", value=(min_d, max_d), min_value=min_d, max_value=max_d,
        label_visibility="collapsed",
    )

    st.markdown("---")
    all_states = sorted(df["customer_state"].unique())
    st.markdown("**Bang của khách hàng**")
    st.caption(f"Lọc theo địa lý. Hiện có {len(all_states)} bang.")
    sel_states = st.multiselect(
        "Bang", options=all_states, default=all_states,
        label_visibility="collapsed", placeholder="Chọn bang...",
    )

    st.markdown("---")
    all_cats = sorted(df["product_category_name"].unique())
    st.markdown("**Danh mục sản phẩm**")
    st.caption(f"Lọc theo nhóm hàng. Hiện có {len(all_cats)} danh mục.")
    sel_cats = st.multiselect(
        "Danh mục", options=all_cats, default=all_cats,
        label_visibility="collapsed", placeholder="Chọn danh mục...",
    )

    st.markdown("---")
    st.caption(f"Tổng bản ghi gốc: **{len(df):,}**")


# ──────────────────────────────────────────────
# FILTER
# ──────────────────────────────────────────────
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

revenue      = fdf["total_amount"].sum()
n_orders     = fdf["order_id"].nunique()
avg_order    = revenue / n_orders if n_orders else 0
avg_delivery = fdf["delivery_time_days"].mean()
late_pct     = fdf["is_late"].mean() * 100 if len(fdf) else 0
freight_pct  = fdf["freight_value"].sum() / revenue * 100 if revenue else 0


# ──────────────────────────────────────────────
# HEADER
# ──────────────────────────────────────────────
d0 = date_range[0] if len(date_range) == 2 else "—"
d1 = date_range[1] if len(date_range) == 2 else "—"
st.markdown(f"""
<div class="page-header">
    <h1>Báo cáo Kinh doanh – Olist E-Commerce</h1>
    <p>Dữ liệu lọc: <strong>{len(fdf):,}</strong> giao dịch &nbsp;·&nbsp;
       Từ <strong>{d0}</strong> đến <strong>{d1}</strong></p>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# KPI CARDS
# ──────────────────────────────────────────────
st.markdown('<div class="section-label">Chỉ số kinh doanh tổng hợp</div>', unsafe_allow_html=True)

k1, k2, k3, k4, k5 = st.columns(5)
for col, cls, lbl, val, sub in [
    (k1, "c-blue",  "Tổng Doanh Thu",   f"R$ {revenue:,.0f}",    "Giá trị toàn bộ đơn hàng"),
    (k2, "c-green", "Số Đơn Hàng",      f"{n_orders:,}",          "Đơn hàng duy nhất"),
    (k3, "c-slate", "Giá Trị TB / Đơn", f"R$ {avg_order:,.0f}",  "Doanh thu TB mỗi đơn"),
    (k4, "c-amber", "TG Giao Hàng TB",  f"{avg_delivery:.1f} ng","Ngày từ đặt đến nhận"),
    (k5, "c-red",   "Tỷ Lệ Giao Trễ",  f"{late_pct:.1f}%",      f"Cước vận chuyển: {freight_pct:.1f}% DT"),
]:
    with col:
        st.markdown(f"""
        <div class="kpi-wrap {cls}">
            <div class="kpi-label">{lbl}</div>
            <div class="kpi-number">{val}</div>
            <div class="kpi-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "Xu Hướng Theo Thời Gian",
    "Địa Lý và Danh Mục",
    "Hiệu Suất Giao Hàng",
])


# ══════════════════════════════════════════════
# TAB 1
# ══════════════════════════════════════════════
with tab1:
    monthly = (
        fdf.groupby(["year_month", "month_dt"], as_index=False)
           .agg(revenue=("total_amount","sum"),
                orders=("order_id","nunique"),
                avg_val=("total_amount","mean"))
           .sort_values("month_dt")
    )

    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=monthly["year_month"], y=monthly["revenue"],
        name="Doanh thu tháng", marker_color=BLUE_L,
        hovertemplate="<b>%{x}</b><br>Doanh thu: R$ %{y:,.0f}<extra></extra>",
    ))
    fig1.add_trace(go.Scatter(
        x=monthly["year_month"],
        y=monthly["revenue"].rolling(3, min_periods=1).mean(),
        name="Trung bình 3 tháng", mode="lines",
        line=dict(color=BLUE, width=2, dash="dot"),
        hovertemplate="<b>%{x}</b><br>TB 3 tháng: R$ %{y:,.0f}<extra></extra>",
    ))
    fig1.update_layout(chart_layout(
        "Doanh thu theo tháng — kèm xu hướng trung bình động 3 tháng",
        yaxis_extra=dict(title=dict(text="Doanh thu (R$)", font=AXIS_FONT)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1,
                    bgcolor=WHITE, bordercolor=BORDER, borderwidth=1, font=LEGEND_FONT),
        height=340,
    ))
    st.plotly_chart(fig1, use_container_width=True)

    col_a, col_b = st.columns(2)

    fig2 = go.Figure(go.Scatter(
        x=monthly["year_month"], y=monthly["orders"],
        mode="lines+markers",
        line=dict(color=GREEN, width=2), marker=dict(size=5, color=GREEN),
        fill="tozeroy", fillcolor="rgba(5,150,105,0.07)",
        hovertemplate="<b>%{x}</b><br>Đơn hàng: %{y:,}<extra></extra>",
    ))
    fig2.update_layout(chart_layout(
        "Số đơn hàng theo tháng",
        yaxis_extra=dict(title=dict(text="Số đơn", font=AXIS_FONT)),
        showlegend=False, height=280,
    ))
    col_a.plotly_chart(fig2, use_container_width=True)

    fig3 = go.Figure(go.Scatter(
        x=monthly["year_month"], y=monthly["avg_val"],
        mode="lines+markers",
        line=dict(color=AMBER, width=2), marker=dict(size=5, color=AMBER),
        hovertemplate="<b>%{x}</b><br>Giá trị TB: R$ %{y:,.0f}<extra></extra>",
    ))
    fig3.update_layout(chart_layout(
        "Giá trị trung bình mỗi đơn hàng",
        yaxis_extra=dict(title=dict(text="R$", font=AXIS_FONT)),
        showlegend=False, height=280,
    ))
    col_b.plotly_chart(fig3, use_container_width=True)

    best = monthly.loc[monthly["revenue"].idxmax()]
    st.markdown(f"""
    <div class="insight-box">
        <strong>Nhận xét:</strong> Tháng đạt doanh thu cao nhất là
        <strong>{best['year_month']}</strong> với <strong>R$ {best['revenue']:,.0f}</strong>.
        Đường trung bình động 3 tháng giúp loại bỏ biến động ngắn hạn và
        phản ánh xu hướng tăng trưởng thực sự của doanh nghiệp.
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════
# TAB 2
# ══════════════════════════════════════════════
with tab2:
    col_c, col_d = st.columns(2)

    state_rev = (
        fdf.groupby("customer_state", as_index=False)
           .agg(revenue=("total_amount","sum"), orders=("order_id","nunique"))
           .sort_values("revenue", ascending=False).head(12)
    )
    state_rev["pct"] = state_rev["revenue"] / state_rev["revenue"].sum() * 100

    fig4 = go.Figure(go.Bar(
        x=state_rev["revenue"], y=state_rev["customer_state"],
        orientation="h",
        marker=dict(color=state_rev["revenue"],
                    colorscale=[[0,"#DBEAFE"],[1,"#1D4ED8"]], showscale=False),
        text=state_rev["pct"].map("{:.1f}%".format),
        textposition="outside",
        textfont=dict(size=11, color=BLACK),   # chu DEN ro rang
        hovertemplate="<b>Bang %{y}</b><br>Doanh thu: R$ %{x:,.0f}<extra></extra>",
    ))
    fig4.update_layout(chart_layout(
        "Doanh thu theo bang khách hàng (Top 12)",
        xaxis_extra=dict(title=dict(text="Doanh thu (R$)", font=AXIS_FONT)),
        yaxis_extra=dict(categoryorder="total ascending", showgrid=False, tickfont=TICK_FONT),
        height=420,
    ))
    col_c.plotly_chart(fig4, use_container_width=True)

    cat_rev = (
        fdf.groupby("product_category_name", as_index=False)
           .agg(revenue=("total_amount","sum"), orders=("order_id","nunique"))
           .sort_values("revenue", ascending=False).head(12)
    )

    fig5 = go.Figure(go.Bar(
        x=cat_rev["revenue"], y=cat_rev["product_category_name"],
        orientation="h",
        marker=dict(color=cat_rev["revenue"],
                    colorscale=[[0,"#D1FAE5"],[1,"#059669"]], showscale=False),
        hovertemplate="<b>%{y}</b><br>Doanh thu: R$ %{x:,.0f}<extra></extra>",
    ))
    fig5.update_layout(chart_layout(
        "Doanh thu theo danh mục sản phẩm (Top 12)",
        xaxis_extra=dict(title=dict(text="Doanh thu (R$)", font=AXIS_FONT)),
        yaxis_extra=dict(categoryorder="total ascending", showgrid=False, tickfont=TICK_FONT),
        height=420,
    ))
    col_d.plotly_chart(fig5, use_container_width=True)

    st.markdown('<div class="section-label">Phân tích chéo: Bang x Danh mục (Top 8 x Top 8)</div>',
                unsafe_allow_html=True)

    top8_states = state_rev["customer_state"].head(8).tolist()
    top8_cats   = cat_rev["product_category_name"].head(8).tolist()
    heat_df = (
        fdf[fdf["customer_state"].isin(top8_states) &
            fdf["product_category_name"].isin(top8_cats)]
        .groupby(["customer_state","product_category_name"], as_index=False)
        .agg(revenue=("total_amount","sum"))
        .pivot(index="customer_state", columns="product_category_name", values="revenue")
        .fillna(0)
    )

    fig6 = go.Figure(go.Heatmap(
        z=heat_df.values,
        x=heat_df.columns.tolist(),
        y=heat_df.index.tolist(),
        colorscale=[[0,"#EFF6FF"],[0.5,"#60A5FA"],[1,"#1D4ED8"]],
        hovertemplate="Bang: <b>%{y}</b><br>Danh mục: <b>%{x}</b><br>Doanh thu: R$ %{z:,.0f}<extra></extra>",
        showscale=True,
        colorbar=dict(
            tickfont=dict(size=10, color=BLACK),          # chu den tren colorbar
            title=dict(text="R$", side="right",
                       font=dict(size=12, color=BLACK)),  # title colorbar den
        ),
    ))
    fig6.update_layout(chart_layout(
        "Bản đồ nhiệt: Doanh thu theo Bang x Danh mục",
        xaxis_extra=dict(
            tickangle=0,                                  # ngang, khong xoay
            tickfont=dict(size=10, color=BLACK),
            showgrid=False,
        ),
        yaxis_extra=dict(
            tickfont=dict(size=11, color=BLACK),
            showgrid=False,
        ),
        height=360,
        margin=dict(l=16, r=100, t=56, b=130),
    ))
    st.plotly_chart(fig6, use_container_width=True)

    # ── Top Seller ──
    st.markdown('<div class="section-label">Top Seller theo Doanh thu</div>',
                unsafe_allow_html=True)

    seller_df = (
        fdf.groupby("seller_id", as_index=False)
           .agg(
               doanh_thu=("total_amount", "sum"),
               so_don=("order_id", "nunique"),
               gia_tri_tb=("total_amount", "mean"),
           )
           .sort_values("doanh_thu", ascending=False)
           .head(15)
           .reset_index(drop=True)
    )
    seller_df.index += 1  # bat dau tu 1
    tong = seller_df["doanh_thu"].sum()
    seller_df["% tong"] = (seller_df["doanh_thu"] / fdf["total_amount"].sum() * 100)

    col_s1, col_s2 = st.columns([1.1, 1])

    # Bar chart ngang
    fig_sel = go.Figure(go.Bar(
        x=seller_df["doanh_thu"],
        y=seller_df["seller_id"].str[:12] + "…",
        orientation="h",
        marker=dict(
            color=seller_df["doanh_thu"],
            colorscale=[[0, "#E0E7FF"], [1, "#4338CA"]],
            showscale=False,
        ),
        text=seller_df["doanh_thu"].map("R$ {:,.0f}".format),
        textposition="outside",
        textfont=dict(size=10, color=BLACK),
        hovertemplate=(
            "<b>Seller:</b> %{y}<br>"
            "Doanh thu: R$ %{x:,.0f}<extra></extra>"
        ),
    ))
    fig_sel.update_layout(chart_layout(
        "Top 15 Seller theo Doanh thu",
        xaxis_extra=dict(title=dict(text="Doanh thu (R$)", font=AXIS_FONT)),
        yaxis_extra=dict(categoryorder="total ascending", showgrid=False,
                         tickfont=dict(size=10, color=BLACK)),
        height=460,
        margin=dict(l=50, r=120, t=60, b=50),
    ))
    col_s1.plotly_chart(fig_sel, use_container_width=True)

    # Bảng số liệu chi tiết
    with col_s2:
        st.markdown("""
        <div style="font-size:13px; font-weight:600; color:#111827;
                    margin-bottom:10px; margin-top:8px;">
            Chi tiết Top 15 Seller
        </div>
        """, unsafe_allow_html=True)

        table_df = seller_df[["seller_id", "doanh_thu", "so_don", "gia_tri_tb", "% tong"]].copy()
        table_df["seller_id"] = table_df["seller_id"].str[:16] + "…"

        st.dataframe(
            table_df,
            use_container_width=True,
            height=420,
            column_config={
                "seller_id":   st.column_config.TextColumn("Seller ID"),
                "doanh_thu":   st.column_config.NumberColumn(
                    "Doanh thu (R$)", format="R$ %.0f"
                ),
                "so_don":      st.column_config.NumberColumn("Số đơn"),
                "gia_tri_tb":  st.column_config.NumberColumn(
                    "Giá trị TB (R$)", format="R$ %.0f"
                ),
                "% tong":      st.column_config.ProgressColumn(
                    "% tổng DT", min_value=0, max_value=100, format="%.1f%%"
                ),
            },
        )

        # Insight nhanh
        top1 = seller_df.iloc[0]
        st.markdown(f"""
        <div class="insight-box" style="margin-top:10px;">
            <strong>Nhận xét:</strong> Seller dẫn đầu chiếm
            <strong>{top1['% tong']:.1f}%</strong> tổng doanh thu với
            <strong>{top1['so_don']:,} đơn</strong>.
            Top 5 seller cộng lại chiếm
            <strong>{seller_df.head(5)['% tong'].sum():.1f}%</strong>
            — mức độ tập trung này cần theo dõi để tránh rủi ro phụ thuộc.
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════
# TAB 3
# ══════════════════════════════════════════════
with tab3:
    col_e, col_f = st.columns([1.4, 1])

    del_clean = fdf["delivery_time_days"].dropna()

    fig7 = go.Figure()
    fig7.add_trace(go.Histogram(
        x=del_clean, nbinsx=35,
        marker_color=BLUE_L, marker_line_color=BLUE, marker_line_width=0.5,
        hovertemplate="Thời gian %{x} ngày: %{y:,} đơn<extra></extra>",
    ))
    fig7.add_vline(
        x=del_clean.mean(), line_width=2, line_dash="dash", line_color=BLUE,
        annotation_text=f"TB: {del_clean.mean():.1f} ngày",
        annotation_position="top right",
        annotation_font=dict(color=BLUE, size=12, family="IBM Plex Sans"),
    )
    fig7.add_vline(
        x=del_clean.median(), line_width=1.5, line_dash="dot", line_color=SLATE,
        annotation_text=f"Trung vị: {del_clean.median():.1f} ngày",
        annotation_position="top left",
        annotation_font=dict(color=SLATE, size=11, family="IBM Plex Sans"),
    )
    fig7.update_layout(chart_layout(
        "Phân phối thời gian giao hàng",
        xaxis_extra=dict(title=dict(text="Số ngày giao", font=AXIS_FONT)),
        yaxis_extra=dict(title=dict(text="Số đơn hàng", font=AXIS_FONT)),
        showlegend=False, height=320,
    ))
    col_e.plotly_chart(fig7, use_container_width=True)

    late_df = (
        fdf.assign(status=fdf["is_late"].map({1:"Giao trễ", 0:"Đúng hẹn"}))
           .groupby("status", as_index=False).agg(n=("order_id","nunique"))
    )
    fig8 = go.Figure(go.Pie(
        labels=late_df["status"], values=late_df["n"],
        hole=0.55,
        marker=dict(colors=[RED, GREEN], line=dict(color=WHITE, width=2)),
        textfont=dict(size=13, color=WHITE),    # chu TRANG tren manh mau
        hovertemplate="<b>%{label}</b>: %{value:,} đơn (%{percent})<extra></extra>",
    ))
    fig8.update_layout(chart_layout(
        "Tỷ lệ giao hàng đúng hẹn",
        showlegend=True, height=320,
        legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.05,
                    font=LEGEND_FONT, bgcolor=WHITE, bordercolor=BORDER, borderwidth=1),
        annotations=[dict(
            text=f"<b>{late_pct:.1f}%</b><br>giao trễ",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color=RED, family="IBM Plex Mono"),
        )],
    ))
    col_f.plotly_chart(fig8, use_container_width=True)

    # Bar: ty le tre theo bang — colorbar phai co chu den
    state_late = (
        fdf.groupby("customer_state")
           .agg(late_rate=("is_late","mean"), orders=("order_id","nunique"))
           .reset_index().query("orders >= 50")
           .sort_values("late_rate", ascending=False).head(15)
    )
    state_late["late_pct_val"] = state_late["late_rate"] * 100

    fig9 = go.Figure(go.Bar(
        x=state_late["customer_state"], y=state_late["late_pct_val"],
        marker=dict(
            color=state_late["late_pct_val"],
            colorscale=[[0,"#FEF3C7"],[0.5,"#F59E0B"],[1,"#DC2626"]],
            showscale=True,
            colorbar=dict(
                title=dict(text="%", font=dict(size=12, color=BLACK)),   # den
                tickfont=dict(size=10, color=BLACK),                      # den
            ),
        ),
        hovertemplate="<b>Bang %{x}</b><br>Tỷ lệ trễ: %{y:.1f}%<extra></extra>",
    ))
    fig9.add_hline(
        y=late_pct, line_dash="dash", line_color=SLATE, line_width=1.5,
        annotation_text=f"TB toàn quốc: {late_pct:.1f}%",
        annotation_font=dict(size=11, color=BLACK, family="IBM Plex Sans"),
    )
    fig9.update_layout(chart_layout(
        "Tỷ lệ giao trễ theo bang (bang có >= 50 đơn)",
        xaxis_extra=dict(title=dict(text="Bang", font=AXIS_FONT), tickfont=TICK_FONT),
        yaxis_extra=dict(title=dict(text="Tỷ lệ trễ (%)", font=AXIS_FONT)),
        showlegend=False, height=320,
    ))
    st.plotly_chart(fig9, use_container_width=True)

    col_g, col_h = st.columns(2)
    col_g.markdown(f"""
    <div class="insight-box">
        <strong>Giao hàng:</strong> Trung bình <strong>{del_clean.mean():.1f} ngày</strong>,
        trung vị <strong>{del_clean.median():.1f} ngày</strong>.
        Tỷ lệ trễ <strong>{late_pct:.1f}%</strong> — cần theo dõi đặc biệt các bang
        vượt mức trung bình toàn quốc.
    </div>
    """, unsafe_allow_html=True)
    col_h.markdown(f"""
    <div class="insight-box">
        <strong>Chi phí vận chuyển:</strong> Chiếm <strong>{freight_pct:.1f}%</strong>
        tổng doanh thu. Mức lý tưởng trong thương mại điện tử thường dưới 10% —
        cần tối ưu logistics nếu chỉ số này cao hơn ngưỡng đó.
    </div>
    """, unsafe_allow_html=True)


# ──────────────────────────────────────────────
# BANG CHI TIET
# ──────────────────────────────────────────────
with st.expander("Xem dữ liệu chi tiết — 500 dòng đầu", expanded=False):
    show = fdf[[
        "order_id","customer_state","product_category_name","seller_state",
        "price","freight_value","total_amount",
        "delivery_time_days","delivery_delay_days","is_late",
        "order_purchase_timestamp",
    ]].head(500).copy()
    show["is_late"] = show["is_late"].map({1:"Trễ", 0:"Đúng hẹn"})

    st.dataframe(show, use_container_width=True, height=380,
        column_config={
            "order_id":                 st.column_config.TextColumn("Mã đơn"),
            "customer_state":           st.column_config.TextColumn("Bang KH"),
            "seller_state":             st.column_config.TextColumn("Bang Seller"),
            "product_category_name":    st.column_config.TextColumn("Danh mục"),
            "price":                    st.column_config.NumberColumn("Giá (R$)", format="R$ %.2f"),
            "freight_value":            st.column_config.NumberColumn("Cước (R$)", format="R$ %.2f"),
            "total_amount":             st.column_config.NumberColumn("Tổng (R$)", format="R$ %.2f"),
            "delivery_time_days":       st.column_config.ProgressColumn("Ngày giao", min_value=0, max_value=60, format="%d ng"),
            "delivery_delay_days":      st.column_config.NumberColumn("Trễ (ng)"),
            "is_late":                  st.column_config.TextColumn("Trạng thái"),
            "order_purchase_timestamp": st.column_config.DatetimeColumn("Ngày đặt", format="DD/MM/YYYY"),
        },
    )

st.markdown("""
<div style="text-align:center; padding:28px 0 8px; font-size:11px; color:#6B7280; letter-spacing:0.8px;">
    Olist Sales Intelligence &nbsp;·&nbsp; Data Warehouse Analytics Platform
</div>
""", unsafe_allow_html=True)