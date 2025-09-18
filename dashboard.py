# dashboard.py
# ---------------------------------------------------------------
# Risk Radar — Banker-Friendly Dashboard for your SQLite schema
# trades:      counterparty, timestamp (TEXT), country, currency, ...
# risk_scores: base_rule_score, ml_anomaly_score, combined_score, decision, reason, created_at
# alerts:      severity, message, created_at
# ---------------------------------------------------------------

import os
import sqlite3
from typing import Tuple

import pandas as pd
import altair as alt
import streamlit as st

DB_PATH = os.getenv("RISK_DB", "risk_demo.sqlite")

# -------------------- Page setup --------------------
st.set_page_config(
    page_title="Risk Radar Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------- Styling helpers --------------------
SEV_COLORS = {"INFO": "#2b8a3e", "WARNING": "#e67700", "CRITICAL": "#c92a2a"}
DEC_EMOJI = {"ALLOW": "🟢", "REVIEW": "🟠", "BLOCK": "🔴"}

def tag(label, color):
    st.markdown(
        f"""<span style="background:{color}; color:white;
                    padding:4px 10px; border-radius:999px;
                    font-size:0.85rem">{label}</span>""",
        unsafe_allow_html=True,
    )

# -------------------- Sidebar: Branding & Currency --------------------
st.sidebar.title("Risk Radar")
st.sidebar.caption("Simple, real-time command center for financial risk.")

# Logo upload + size
logo = st.sidebar.file_uploader("Add your bank logo (optional)", type=["png", "jpg", "jpeg"])
logo_width = st.sidebar.slider("Logo width (px)", 60, 240, 120)
if logo:
    # Sidebar preview
    st.sidebar.image(logo, caption="Logo", use_container_width=True)

# Currency presentation
DISPLAY_CCY = st.sidebar.selectbox("Show amounts in", ["USD ($)", "INR (₹)"], index=1)
usd_to_inr = st.sidebar.number_input("USD → INR rate", min_value=50.0, max_value=150.0, value=83.0, step=0.1)

def money_disp(x: float) -> str:
    """Format amounts in the sidebar-selected currency."""
    try:
        amt = float(x or 0.0)
    except Exception:
        return "-"
    if DISPLAY_CCY.startswith("INR"):
        amt = amt * usd_to_inr  # treat stored notional as USD → convert to INR
        return f"₹{amt:,.0f}"
    return f"${amt:,.0f}"

st.sidebar.button("🔄 Refresh")

# -------------------- Data access --------------------
def read_sql(query: str, params=()):
    with sqlite3.connect(DB_PATH) as con:
        return pd.read_sql_query(query, con, params=params)

@st.cache_data(ttl=5.0)
def load_core() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # trades
    trades = read_sql("""
        SELECT trade_id, timestamp, counterparty, sector, country, symbol, trade_type,
               quantity, price, notional, currency, kyc_ok, aml_flag, status
        FROM trades
    """)

    # risk_scores (map old -> friendly names)
    scores = read_sql("""
        SELECT trade_id,
               base_rule_score  AS rule_score,
               ml_anomaly_score AS ml_score,
               combined_score,
               decision,
               reason           AS reasons,
               created_at
        FROM risk_scores
    """)
    # derive severity from decision
    decision_to_sev = {"BLOCK": "CRITICAL", "REVIEW": "WARNING", "ALLOW": "INFO"}
    scores["severity"] = scores["decision"].map(decision_to_sev).fillna("INFO")

    # alerts
    alerts = read_sql("""
        SELECT created_at AS time, trade_id, severity, message
        FROM alerts
        ORDER BY time DESC
        LIMIT 2000
    """)

    return trades, scores, alerts

# -------------------- Load & normalize --------------------
trades, scores, alerts = load_core()
df = trades.merge(scores, on="trade_id", how="left")

# Parse timestamps as naive (avoid tz mismatch)
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.tz_localize(None)
alerts["time"] = pd.to_datetime(alerts["time"], errors="coerce").dt.tz_localize(None)

# -------------------- Header --------------------
if logo:
    st.image(logo, width=logo_width)  # small header logo

tabs = st.tabs(["🏁 Overview", "📊 My Exposure", "🧪 Stress Test", "📜 Alerts Log"])

# ============================================================
# 1) OVERVIEW
# ============================================================
with tabs[0]:
    st.markdown("# 🛡️ Risk Radar — Banker View")
    st.caption("Plain-English status, key metrics, and what changed recently.")

    # KPI cards
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1, 1])
    total_deals = len(df)
    total_value = df["notional"].sum() if "notional" in df.columns else 0.0
    review_cnt = int((df["decision"] == "REVIEW").sum()) if "decision" in df.columns else 0
    critical_cnt = int((df["severity"] == "CRITICAL").sum()) if "severity" in df.columns else 0

    with c1:
        st.metric("Total Deals", f"{total_deals:,}", help="Number of trades processed so far.")
        st.caption("How many trades we’ve processed.")
    with c2:
        st.metric("Total Value", money_disp(total_value),
                  help="Total notional value of all trades, shown in the currency selected in the sidebar.")
        st.caption("Sum of trade notionals.")
    with c3:
        st.metric("Needs Review", f"{review_cnt:,}",
                  help="Trades that the system recommends a human to check.")
        st.caption("Traders / Ops to check.")
    with c4:
        st.metric("Critical Alerts", f"{critical_cnt:,}",
                  help="High severity situations. These usually require immediate action.")
        st.caption("Immediate attention required.")

    # Executive Summary (safe markdown)
    st.markdown("### 📌 Executive Summary")
    now = pd.Timestamp.now()
    recent = df[df["timestamp"] >= (now - pd.Timedelta(minutes=15))] if "timestamp" in df.columns else df.iloc[0:0]
    last_15m_value = float(recent["notional"].sum()) if not recent.empty else 0.0
    top_ccy = (df.groupby("currency")["notional"].sum().sort_values(ascending=False).head(1)
               if "currency" in df.columns else pd.Series([], dtype=float))
    top_ccy_label = top_ccy.index[0] if len(top_ccy) else "—"
    top_ccy_amt   = float(top_ccy.iloc[0]) if len(top_ccy) else 0.0
    sev_counts = df["severity"].value_counts().to_dict() if "severity" in df.columns else {}

    summary_md = (
        f"Markets look **stable** overall. "
        f"We processed **{total_deals:,} deals** worth **{money_disp(total_value)}**. "
        f"In the last 15 minutes we saw **{money_disp(last_15m_value)}** of new flow. "
        f"Biggest exposure is in **{top_ccy_label} ({money_disp(top_ccy_amt)})**.\n\n"
        f"Alert mix: **{sev_counts.get('CRITICAL',0)}** critical, "
        f"**{sev_counts.get('WARNING',0)}** warning, **{sev_counts.get('INFO',0)}** info."
    )
    st.info(summary_md)

    # Decisions pie
    if "decision" in df.columns and not df["decision"].dropna().empty:
        pie_src = df.dropna(subset=["decision"]).groupby("decision").size().reset_index(name="count")
        pie = alt.Chart(pie_src).mark_arc(innerRadius=60).encode(
            theta="count:Q",
            color=alt.Color("decision:N",
                            scale=alt.Scale(domain=list(DEC_EMOJI.keys()),
                                            range=["#2f9e44", "#f08c00", "#c92a2a"])),
            tooltip=["decision:N", "count:Q"]
        ).properties(height=260)
        st.altair_chart(pie, use_container_width=True)

    # Top reasons
    st.markdown("### 🔍 What drove today’s risk?")
    if "reasons" in df.columns and not df["reasons"].dropna().empty:
        reasons = (
            df["reasons"].dropna().str.split(";", expand=True).stack().str.strip()
            .value_counts().reset_index()
        )
        reasons.columns = ["Reason", "Count"]
        bars = alt.Chart(reasons.head(8)).mark_bar().encode(
            x="Count:Q",
            y=alt.Y("Reason:N", sort='-x'),
            tooltip=["Reason", "Count"]
        ).properties(height=260)
        st.altair_chart(bars, use_container_width=True)
    else:
        st.caption("No rule violations captured yet.")

    # Glossary (Overview)
    with st.expander("ℹ️ Glossary of terms (tap to open)"):
        st.markdown("""
- **Total Deals**: How many trades the system processed.
- **Total Value**: The sum of all deal notionals (face value of trades).
- **Needs Review**: Trades where automated rules/ML suggest human oversight.
- **Critical Alerts**: High-severity issues that need immediate action.
- **Notional**: The base size or face value of a financial transaction.
- **Decision**: The system’s classification of a trade (ALLOW, REVIEW, BLOCK).
- **Severity**: Risk importance level (INFO, WARNING, CRITICAL).
- **Reasons**: Why a trade was flagged (limits, KYC/AML, restricted country, etc.).
""")

# ============================================================
# 2) EXPOSURE
# ============================================================
with tabs[1]:
    st.markdown("## 📊 Our Financial Exposure")
    cA, cB, cC = st.columns(3)
    with cA:
        st.metric("Currencies Held", df["currency"].nunique() if "currency" in df.columns else 0,
                  help="How many different currencies appear in the book.")
        st.caption("Count of distinct currencies.")
    with cB:
        st.metric("Countries in Book", df["country"].nunique() if "country" in df.columns else 0,
                  help="Number of countries involved in recent trades.")
        st.caption("Distinct countries in activity.")
    with cC:
        st.metric("Counterparties", df["counterparty"].nunique() if "counterparty" in df.columns else 0,
                  help="Unique trading partners seen in the data.")
        st.caption("Unique partner entities.")

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("By Currency")
        if "currency" in df.columns and "notional" in df.columns:
            # convert for display if INR selected
            cdf = df.copy()
            factor = usd_to_inr if DISPLAY_CCY.startswith("INR") else 1.0
            cdf["notional_display"] = cdf["notional"] * factor
            ccy = (cdf.groupby("currency")["notional_display"]
                   .sum().reset_index().sort_values("notional_display", ascending=False))
            if not ccy.empty:
                chart = alt.Chart(ccy).mark_bar().encode(
                    x=alt.X("currency:N", title="Currency"),
                    y=alt.Y("notional_display:Q", title=f"Total Value ({'₹' if DISPLAY_CCY.startswith('INR') else '$'})"),
                    tooltip=[alt.Tooltip("currency:N", title="Currency"),
                             alt.Tooltip("notional_display:Q", title="Total Value", format=",.0f")]
                ).properties(height=320)
                st.altair_chart(chart, use_container_width=True)

    with right:
        st.subheader("Top Countries")
        if "country" in df.columns and "notional" in df.columns:
            factor = usd_to_inr if DISPLAY_CCY.startswith("INR") else 1.0
            cdf = df.copy()
            cdf["notional_display"] = cdf["notional"] * factor
            country = (cdf.groupby("country")["notional_display"]
                       .sum().reset_index().sort_values("notional_display", ascending=False).head(10))
            if not country.empty:
                chart2 = alt.Chart(country).mark_bar().encode(
                    x=alt.X("notional_display:Q", title=f"Total Value ({'₹' if DISPLAY_CCY.startswith('INR') else '$'})"),
                    y=alt.Y("country:N", sort='-x', title="Country"),
                    tooltip=[alt.Tooltip("country:N", title="Country"),
                             alt.Tooltip("notional_display:Q", title="Total Value", format=",.0f")]
                ).properties(height=320)
                st.altair_chart(chart2, use_container_width=True)

    # Glossary (Exposure)
    with st.expander("ℹ️ Glossary of terms (tap to open)"):
        st.markdown("""
- **Currencies Held**: Number of different currencies currently in the portfolio.
- **Countries in Book**: Distinct countries involved in recent trades.
- **Counterparties**: Unique trading partners recorded.
- **Exposure**: The total value at risk across trades in a given dimension (currency/country).
- **FX**: Short for *Foreign Exchange*, i.e. currency risk.
""")

# ============================================================
# 3) STRESS TEST (scenario presets)
# ============================================================
with tabs[2]:
    st.markdown("## 🧪 'What-If' Simulator")

    presets = {
        "Mild Shock (FX −3%, Price −5%)": (-3, -5),
        "Bear Case (FX −10%, Price −15%)": (-10, -15),
        "Crisis Case (FX −20%, Price −30%)": (-20, -30),
        "Rally (FX +5%, Price +8%)": (5, 8),
    }
    b1, b2, b3, b4 = st.columns(4)
    for i, (name, (fxv, pxv)) in enumerate(presets.items()):
        (b1, b2, b3, b4)[i].button(
            name,
            key=f"preset_{i}",
            on_click=lambda f=fxv, p=pxv: st.session_state.update({"fx": f, "px": p})
        )

    fx = st.slider("Change in Currency Rates (%)", -30, 30, st.session_state.get("fx", 0),
                   help="Simulate a percentage move in FX rates. Negative = depreciation vs USD.")
    px = st.slider("Change in Asset Prices (%)", -30, 30, st.session_state.get("px", 0),
                   help="Simulate a percentage move in asset prices. Negative = price drop.")

    if "currency" in df.columns and "notional" in df.columns:
        base = (
            df.groupby("currency")["notional"].sum()
            .reset_index().rename(columns={"notional": "base"})
        )
    else:
        base = pd.DataFrame(columns=["currency", "base"])

    st.caption(f"Δ shows change vs current book. FX: {fx:+d}%  |  Prices: {px:+d}%")

    if not base.empty:
        # scenario impact on notionals (in USD). For display, convert.
        base["shock"] = base["base"] * ((100 + fx) / 100.0) * ((100 + px) / 100.0)
        factor = usd_to_inr if DISPLAY_CCY.startswith("INR") else 1.0
        disp = base.copy()
        disp["base"] = disp["base"] * factor
        disp["shock"] = disp["shock"] * factor

        shock_chart = alt.Chart(disp).transform_fold(
            ["base", "shock"], as_=["Scenario", "Value"]
        ).mark_bar().encode(
            x=alt.X("currency:N", title="Currency"),
            y=alt.Y("Value:Q", title=f"Total Value ({'₹' if DISPLAY_CCY.startswith('INR') else '$'})"),
            color=alt.Color("Scenario:N", scale=alt.Scale(range=["#8d99ae", "#e76f51"])),
            tooltip=[alt.Tooltip("currency:N"),
                     alt.Tooltip("Scenario:N"),
                     alt.Tooltip("Value:Q", format=",.0f")]
        ).properties(height=340)
        st.altair_chart(shock_chart, use_container_width=True)

        total_base = float(disp["base"].sum())
        total_shock = float(disp["shock"].sum())
        delta = total_shock - total_base
        pct = (delta / total_base * 100) if total_base else 0.0
        st.metric("Portfolio P&L under Scenario", money_disp(delta),
                  delta=f"{pct:,.2f}%")

    # Glossary (Stress Test)
    with st.expander("ℹ️ Glossary of terms (tap to open)"):
        st.markdown("""
- **Stress Test**: Simulates extreme but plausible market scenarios.
- **FX Shock**: A sudden percentage move in exchange rates.
- **Price Shock**: A simulated increase/decrease in asset prices.
- **Scenario Presets**: Predefined combinations of shocks (mild, crisis, rally).
- **Portfolio P&L**: Profit or Loss of the whole book under the simulated scenario.
""")

# ============================================================
# 4) ALERTS LOG (cards + explainability chips)
# ============================================================
with tabs[3]:
    st.markdown("## 📜 Alerts & Warnings Log")

    # Build alert view from processed trades + scores
    base_cols = ["timestamp", "trade_id", "decision", "severity", "reasons", "combined_score",
                 "counterparty", "currency", "notional", "kyc_ok", "aml_flag", "country"]
    avail = [c for c in base_cols if c in df.columns]
    df_alerts = df[avail].copy()
    if "timestamp" in df_alerts.columns:
        df_alerts.rename(columns={"timestamp": "time"}, inplace=True)
        df_alerts["time"] = pd.to_datetime(df_alerts["time"], errors="coerce").dt.tz_localize(None)
        df_alerts = df_alerts.sort_values("time", ascending=False)

    # Filters
    f1, f2, f3 = st.columns([1, 1, 2])
    sev_filter = f1.selectbox("Filter by Severity", ["All", "CRITICAL", "WARNING", "INFO"])
    dec_filter = f2.selectbox("Filter by Decision", ["All", "BLOCK", "REVIEW", "ALLOW"])
    search_txt = f3.text_input("Search by Deal ID / Reason / Counterparty")

    if "severity" in df_alerts.columns and sev_filter != "All":
        df_alerts = df_alerts[df_alerts["severity"] == sev_filter]
    if "decision" in df_alerts.columns and dec_filter != "All":
        df_alerts = df_alerts[df_alerts["decision"] == dec_filter]
    if search_txt:
        mask = False
        for col in ["trade_id", "reasons", "counterparty"]:
            if col in df_alerts.columns:
                m = df_alerts[col].astype(str).str.contains(search_txt, case=False, na=False)
                mask = m if isinstance(mask, bool) else (mask | m)
        if not isinstance(mask, bool):
            df_alerts = df_alerts[mask]

    st.caption(f"Showing {len(df_alerts):,} records")

    # Card view (first 100 for speed)
    for _, row in df_alerts.head(100).iterrows():
        header_parts = []
        if pd.notna(row.get("decision")):
            header_parts.append(f"{DEC_EMOJI.get(row['decision'],'')} {row['decision']}")
        if pd.notna(row.get("trade_id")):
            header_parts.append(str(row["trade_id"]))
        if pd.notna(row.get("time")):
            header_parts.append(str(row["time"]))
        title = " — ".join(header_parts) if header_parts else "Alert"

        with st.expander(title):
            # Severity badge
            if pd.notna(row.get("severity")):
                tag(str(row["severity"]), SEV_COLORS.get(str(row["severity"]), "#495057"))

            # Score
            if pd.notna(row.get("combined_score")):
                st.write(f"**Combined Score:** {float(row['combined_score']):.2f}")

            # Explainability chips
            st.write("**Why this alert?**")
            chips = [r.strip() for r in str(row.get("reasons", "") or "").split(";") if r and r.strip()]
            if chips:
                for ch in chips:
                    tag(ch, "#364fc7")
            else:
                st.caption("No rule violations. Likely ML anomaly or threshold.")

            # Quick context
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Counterparty", str(row.get("counterparty", "-")))
            c2.metric("Currency", str(row.get("currency", "-")))
            c3.metric("Notional", money_disp(row.get("notional", 0)))
            kyc = row.get("kyc_ok", 0)
            aml = row.get("aml_flag", 0)
            try:
                kyc_txt = "✅" if int(kyc) == 1 else "❌"
            except Exception:
                kyc_txt = "—"
            try:
                aml_txt = "🚩" if int(aml) == 1 else "—"
            except Exception:
                aml_txt = "—"
            c4.metric("KYC / AML", f"{kyc_txt} / {aml_txt}")
            if pd.notna(row.get("country")):
                st.caption(f"Country: {row['country']}")

    # Download the currently filtered view
    st.download_button(
        "⬇️ Download current view (CSV)",
        data=df_alerts.to_csv(index=False).encode("utf-8"),
        file_name="alerts_view.csv",
        mime="text/csv",
    )

    # Glossary (Alerts)
    with st.expander("ℹ️ Glossary of terms (tap to open)"):
        st.markdown("""
- **Alerts Log**: History of risk alerts generated by the system.
- **Decision**: System action suggestion (ALLOW, REVIEW, BLOCK).
- **Severity**: Risk level (INFO, WARNING, CRITICAL).
- **Reason Chips**: Short explanations (limit breach, AML flagged, etc.).
- **KYC**: Know Your Customer – regulatory identity verification.
- **AML**: Anti-Money Laundering – system checks for suspicious flows.
""")
