"""Streamlit console for the Lineage Intelligence Pipeline.

The console: global sidebar filters that live-update every view, KPI cards
with period-over-period deltas, Plotly animated charts (animation_frame),
a genuine 3D trade space and a 3D lineage graph (Scatter3d), and a DQ /
run-history drilldown — all over the project's own DuckDB observability
tables and the raw trade / position feeds in ``data/raw``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard.data_bootstrap import ensure_demo_database

DUCKDB_PATH = ensure_demo_database()

st.set_page_config(page_title="Lineage Intelligence", page_icon="🔗", layout="wide")

# ----------------------------------------------------------------------------
# Custom CSS — dark glass / gradient console styling
# ----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(1200px 600px at 15% -10%, #0f3d4a 0%, transparent 55%),
                    radial-gradient(1000px 500px at 90% 0%, #3b1158 0%, transparent 50%),
                    #0e1117;
    }
    .li-hero {
        padding: 1.1rem 1.4rem; border-radius: 16px; margin-bottom: 0.6rem;
        background: linear-gradient(120deg, rgba(16,110,130,.55), rgba(110,40,180,.45));
        border: 1px solid rgba(120,220,255,.35);
        box-shadow: 0 8px 32px rgba(0,0,0,.45);
    }
    .li-hero h1 {
        margin: 0; font-size: 2.1rem; letter-spacing: .5px;
        background: linear-gradient(90deg,#8ff5e2,#9fd0ff,#e3b3ff);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .li-hero p { margin: .25rem 0 0 0; color: #cfd8e3; font-size: .95rem; }
    div[data-testid="stMetric"] {
        background: rgba(22,27,34,.85); border: 1px solid rgba(110,220,254,.35);
        border-radius: 12px; padding: .6rem .8rem;
        box-shadow: 0 4px 18px rgba(0,0,0,.35);
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(227,179,255,.7); }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(22,27,34,.8); border-radius: 10px 10px 0 0; padding: 6px 14px;
    }
    section[data-testid="stSidebar"] { background: rgba(14,17,23,.92); }
    </style>
    <div class="li-hero">
      <h1>🔗 Lineage Intelligence — Data Quality &amp; Provenance Console</h1>
      <p>Column-level lineage, declarative DQ enforcement and pipeline run
         observability for financial trade data. Drag the 3D lineage graph,
         press play on the animated exposure charts, slice everything from
         the sidebar.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

FX_TO_USD = {"EUR": 1.085, "GBP": 1.272, "JPY": 0.0067, "CHF": 1.112,
             "CAD": 0.738, "AUD": 0.653, "USD": 1.0}


def query_df(sql: str, params: List[Any] | None = None) -> pd.DataFrame:
    try:
        import duckdb

        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        df = con.execute(sql, params or []).df()
        con.close()
        return df
    except Exception:
        return pd.DataFrame()


def query_table(sql: str, params: List[Any] | None = None) -> List[dict]:
    df = query_df(sql, params)
    return df.to_dict("records") if not df.empty else []


@st.cache_data(ttl=60)
def load_raw(kind: str) -> pd.DataFrame:
    files = sorted((ROOT_DIR / "data" / "raw").glob(f"{kind}_*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.read_csv(files[-1])


runs = query_df(
    """SELECT batch_id, pipeline_name, stage_name, status, records_in, records_out,
              records_dead, duration_sec, started_at FROM pipeline_runs
       ORDER BY started_at DESC LIMIT 200"""
)
scores = query_df(
    """SELECT batch_id, checkpoint, dq_score, total_violations, critical_count,
              error_count, warn_count, rules_run, rules_failed, recorded_at
       FROM dq_scores ORDER BY recorded_at DESC LIMIT 100"""
)
violations = query_df(
    """SELECT batch_id, rule_name, severity, field_name, offending_value, row_key, stage, ts
       FROM dq_violations ORDER BY ts DESC LIMIT 500"""
)
lineage_edges = query_df(
    "SELECT batch_id, source, target, transform, stage, ts FROM lineage_edges"
)
trades = load_raw("trades")
positions = load_raw("positions")

if not trades.empty:
    trades["trade_date"] = pd.to_datetime(trades["trade_date"], errors="coerce")
    trades["settlement_date"] = pd.to_datetime(trades["settlement_date"], errors="coerce")
    trades["notional"] = pd.to_numeric(trades["notional"], errors="coerce")
    trades["quantity"] = pd.to_numeric(trades["quantity"], errors="coerce")
    trades["fx"] = trades["currency"].map(FX_TO_USD).fillna(1.0)
    trades["notional_usd"] = trades["notional"] * trades["fx"]
    trades["month"] = trades["trade_date"].dt.to_period("M").dt.to_timestamp()
if not positions.empty:
    positions["as_of_date"] = pd.to_datetime(positions["as_of_date"], errors="coerce")
    positions["market_value_usd"] = pd.to_numeric(positions["market_value_usd"], errors="coerce")
    positions["quantity"] = pd.to_numeric(positions["quantity"], errors="coerce")

if not Path(DUCKDB_PATH).exists():
    st.warning("No DuckDB database found yet. Run the pipeline to populate dashboard data.")

# ----------------------------------------------------------------------------
# Sidebar — global dynamic filters (every view below reads the filtered data)
# ----------------------------------------------------------------------------
st.sidebar.header("🎛️ Live filters")
if trades.empty:
    st.sidebar.info("No raw trade feed found in data/raw.")
    f_trades, f_positions = trades, positions
    sel_severities: List[str] = []
else:
    dmin, dmax = trades["trade_date"].min(), trades["trade_date"].max()
    picked = st.sidebar.date_input(
        "Trade date range", value=(dmin.date(), dmax.date()),
        min_value=dmin.date(), max_value=dmax.date(),
    )
    if isinstance(picked, (list, tuple)) and len(picked) == 2:
        start, end = pd.Timestamp(picked[0]), pd.Timestamp(picked[1])
    else:
        start, end = dmin, dmax
    all_assets = sorted(trades["asset_class"].dropna().unique())
    sel_assets = st.sidebar.multiselect("Asset classes", all_assets, default=all_assets)
    all_ccy = sorted(trades["currency"].dropna().unique())
    sel_ccy = st.sidebar.multiselect("Currencies", all_ccy, default=all_ccy)
    all_status = sorted(trades["settlement_status"].dropna().unique())
    sel_status = st.sidebar.multiselect("Settlement status", all_status, default=all_status)
    all_dir = sorted(trades["direction"].dropna().unique())
    sel_dir = st.sidebar.multiselect("Direction", all_dir, default=all_dir)
    min_abs = st.sidebar.slider("Min |notional| ($M)", 0.0, 50.0, 0.0, 1.0)
    sev_opts = sorted(violations["severity"].dropna().unique()) if not violations.empty else []
    sel_severities = st.sidebar.multiselect("DQ severity", sev_opts, default=sev_opts)

    def _apply(df: pd.DataFrame, *, dated: bool) -> pd.DataFrame:
        if df.empty:
            return df
        out = df
        if dated and "trade_date" in out.columns:
            out = out[(out["trade_date"] >= start) & (out["trade_date"] <= end)]
        if "asset_class" in out.columns and sel_assets:
            out = out[out["asset_class"].isin(sel_assets)]
        if "currency" in out.columns and sel_ccy:
            out = out[out["currency"].isin(sel_ccy)]
        if "settlement_status" in out.columns and sel_status:
            out = out[out["settlement_status"].isin(sel_status)]
        if "direction" in out.columns and sel_dir:
            out = out[out["direction"].isin(sel_dir)]
        if "notional" in out.columns and min_abs > 0:
            out = out[out["notional"].abs() >= min_abs * 1e6]
        return out

    f_trades = _apply(trades, dated=True)
    f_positions = positions
    if not positions.empty:
        f_positions = positions[
            positions["asset_class"].isin(sel_assets or all_assets)
            & positions["currency"].isin(sel_ccy or all_ccy)
            & positions["direction"].isin(sel_dir or all_dir)
        ]

f_violations = violations
if not violations.empty and sel_severities:
    f_violations = violations[violations["severity"].isin(sel_severities)]


def _money(x: float) -> str:
    ax = abs(x)
    if ax >= 1e9:
        return f"${x / 1e9:,.2f}B"
    if ax >= 1e6:
        return f"${x / 1e6:,.1f}M"
    if ax >= 1e3:
        return f"${x / 1e3:,.1f}K"
    return f"${x:,.0f}"


def _style(fig):
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      template="plotly_dark")
    return fig


# ----------------------------------------------------------------------------
# KPI row — with deltas vs the prior equal-length period / previous checkpoint
# ----------------------------------------------------------------------------
if not f_trades.empty:
    span_days = max((f_trades["trade_date"].max() - f_trades["trade_date"].min()).days, 1)
    prior_end = f_trades["trade_date"].min() - pd.Timedelta(days=1)
    prior_start = prior_end - pd.Timedelta(days=span_days)
    prior = trades[(trades["trade_date"] >= prior_start) & (trades["trade_date"] <= prior_end)]
    if sel_assets:
        prior = prior[prior["asset_class"].isin(sel_assets)]
    gross = float(f_trades["notional_usd"].abs().sum())
    prior_gross = float(prior["notional_usd"].abs().sum()) if not prior.empty else 0.0
    fail_rate = float((f_trades["settlement_status"] == "FAILED").mean() * 100)
    latest_dq = float(scores["dq_score"].iloc[0]) if not scores.empty else 0.0
    prev_dq = float(scores["dq_score"].iloc[1]) if len(scores) > 1 else None
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Trades (filtered)", f"{len(f_trades):,}",
              delta=f"{len(f_trades) - len(prior):+,} vs prior period" if not prior.empty else None)
    c2.metric("Gross notional (USD)", _money(gross),
              delta=_money(gross - prior_gross) + " vs prior" if not prior.empty else None)
    c3.metric("Latest DQ score", f"{latest_dq:.1f}",
              delta=f"{latest_dq - prev_dq:+.1f} vs prev checkpoint" if prev_dq is not None else None)
    c4.metric("DQ violations", f"{len(f_violations):,}",
              delta=f"{len(violations):,} logged in batch")
    c5.metric("Lineage edges", f"{len(lineage_edges):,}",
              delta=f"{lineage_edges['source'].nunique() if not lineage_edges.empty else 0} source columns")
    c6.metric("Settlement FAILED", f"{fail_rate:.1f}%",
              delta=f"{int((f_trades['settlement_status'] == 'FAILED').sum()):,} trades", delta_color="inverse")

tab_overview, tab_anim, tab_3d, tab_dq, tab_lineage, tab_runs = st.tabs(
    ["📊 Overview", "🎬 Animated exposure", "🧊 3D lineage & trades",
     "🛡️ DQ violations", "🔗 Lineage explorer", "🕘 Run history"]
)

with tab_overview:
    st.subheader("DQ score by checkpoint")
    if scores.empty:
        st.info("No DQ scores yet — run the pipeline first.")
    else:
        fig = _style(px.bar(scores.iloc[::-1], x="checkpoint", y="dq_score",
                            color="checkpoint", text="dq_score",
                            labels={"dq_score": "DQ score", "checkpoint": "Checkpoint"},
                            title="Data-quality score across pipeline checkpoints"))
        fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig.update_yaxes(range=[0, 115])
        st.plotly_chart(fig, width="stretch")
    left, right = st.columns(2)
    with left:
        st.subheader("Exposure by asset class")
        if not f_trades.empty:
            expo = (f_trades.groupby("asset_class", as_index=False)["notional_usd"]
                    .agg(lambda s: float(s.abs().sum())).rename(columns={"notional_usd": "gross_usd"}))
            st.plotly_chart(_style(px.pie(expo, names="asset_class", values="gross_usd",
                                          hole=0.45, title="Gross USD exposure share")),
                            width="stretch")
    with right:
        st.subheader("Settlement outcomes")
        if not f_trades.empty:
            settle = f_trades["settlement_status"].value_counts().reset_index()
            settle.columns = ["status", "trades"]
            st.plotly_chart(_style(px.bar(settle, x="status", y="trades", color="status",
                                          title="Trades by settlement status")),
                            width="stretch")
    st.subheader("Recent pipeline runs")
    st.dataframe(runs, width="stretch")
    st.subheader("Recent DQ scores")
    st.dataframe(scores, width="stretch")

with tab_anim:
    st.subheader("Press play — monthly traded notional by asset class")
    if f_trades.empty:
        st.info("No trades in the current filter.")
    else:
        monthly = (f_trades.groupby(["month", "asset_class"], as_index=False)
                   .agg(gross_usd=("notional_usd", lambda s: float(s.abs().sum())),
                        trades=("trade_id", "count")))
        monthly["gross_m"] = monthly["gross_usd"] / 1e6
        monthly["frame"] = monthly["month"].dt.strftime("%Y-%m")
        lim = float(monthly["gross_m"].max()) or 1.0
        fig_a = px.bar(monthly.sort_values(["month", "asset_class"]), x="asset_class",
                       y="gross_m", color="asset_class", animation_frame="frame",
                       range_y=[0, lim * 1.15],
                       labels={"gross_m": "Gross notional ($M)", "asset_class": "Asset class"},
                       template="plotly_dark",
                       title="Gross traded notional per month — animated")
        fig_a.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_a.layout.updatemenus:
            fig_a.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 450
        st.plotly_chart(fig_a, width="stretch")

        st.subheader("Cumulative exposure race")
        cum = monthly.sort_values(["asset_class", "month"]).copy()
        cum["cum_m"] = cum.groupby("asset_class")["gross_m"].cumsum()
        fig_c = px.line(cum, x="month", y="cum_m", color="asset_class",
                        animation_frame="frame", markers=True,
                        labels={"cum_m": "Cumulative gross ($M)", "month": "Month"},
                        template="plotly_dark", title="Cumulative gross notional — animated race")
        fig_c.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_c.layout.updatemenus:
            fig_c.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 450
        st.plotly_chart(fig_c, width="stretch")
    st.caption("Animations are native Plotly `animation_frame` charts — hit ▶ Play on each chart.")

with tab_3d:
    st.subheader("🧊 3D lineage graph — drag to rotate, scroll to zoom")
    if lineage_edges.empty:
        st.info("No lineage edges recorded yet.")
    else:
        nodes = sorted(set(lineage_edges["source"]) | set(lineage_edges["target"]))
        stage_of: dict[str, str] = {}
        for _, e in lineage_edges.iterrows():
            stage_of.setdefault(e["source"], str(e["source"]).split(".")[0])
            stage_of[e["target"]] = str(e["target"]).split(".")[0]
        # layer = longest path depth from any root
        depth = {n: 0 for n in nodes}
        for _ in range(len(nodes)):
            for _, e in lineage_edges.iterrows():
                if depth[e["target"]] <= depth[e["source"]]:
                    depth[e["target"]] = depth[e["source"]] + 1
        by_depth: dict[int, list[str]] = {}
        for n in nodes:
            by_depth.setdefault(depth[n], []).append(n)
        pos: dict[str, tuple[float, float, float]] = {}
        for d, group in by_depth.items():
            for i, n in enumerate(sorted(group)):
                pos[n] = (float(d), float(i - (len(group) - 1) / 2), float(depth[n] % 3))
        traces: list = []
        for _, e in lineage_edges.iterrows():
            x0, y0, z0 = pos[e["source"]]
            x1, y1, z1 = pos[e["target"]]
            traces.append(go.Scatter3d(
                x=[x0, x1], y=[y0, y1], z=[z0, z1], mode="lines",
                line=dict(color="rgba(140,220,255,.65)", width=4),
                hoverinfo="text", text=f"{e['source']} → {e['target']} · {e['transform']}",
                showlegend=False))
        traces.append(go.Scatter3d(
            x=[pos[n][0] for n in nodes], y=[pos[n][1] for n in nodes],
            z=[pos[n][2] for n in nodes], mode="markers+text", text=nodes,
            textposition="top center",
            marker=dict(size=9, color=[depth[n] for n in nodes], colorscale="Viridis",
                        line=dict(color="white", width=1)),
            hovertext=[f"{n} · layer {depth[n]} · {stage_of.get(n, '')}" for n in nodes],
            hoverinfo="text", name="Columns"))
        fig3 = go.Figure(data=traces)
        fig3.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                           scene=dict(xaxis_title="Lineage depth", yaxis_title="Column",
                                      zaxis_title="Band",
                                      xaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      yaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      zaxis=dict(backgroundcolor="rgba(20,24,32,.9)")),
                           margin=dict(l=0, r=0, t=10, b=0),
                           title="Column-level lineage in 3D — source → transform → report")
        st.plotly_chart(fig3, width="stretch")
        st.dataframe(lineage_edges, width="stretch")

    st.subheader("3D trade space — quantity × notional × time")
    if not f_trades.empty:
        d3 = f_trades.dropna(subset=["notional", "quantity"]).copy()
        d3["notional_m"] = d3["notional_usd"].abs() / 1e6
        fig_t = px.scatter_3d(d3.sort_values("trade_date"), x="trade_date", y="quantity",
                              z="notional_m", color="asset_class", opacity=0.8,
                              hover_data=["trade_id", "currency", "settlement_status"],
                              labels={"quantity": "Quantity", "notional_m": "|Notional| ($M)",
                                      "trade_date": "Trade date"},
                              template="plotly_dark",
                              title="Every filtered trade in date × quantity × notional space")
        fig_t.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                            scene=dict(xaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                       yaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                       zaxis=dict(backgroundcolor="rgba(20,24,32,.9)")),
                            margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_t, width="stretch")

        st.subheader("Exposure surface — asset class × month terrain")
        surf = (f_trades.pivot_table(index="month", columns="asset_class",
                                     values="notional_usd",
                                     aggfunc=lambda s: float(np.abs(s).sum()))
                .sort_index().fillna(0.0))
        if surf.shape[0] > 2 and surf.shape[1] > 1:
            fig_s = go.Figure(data=[go.Surface(
                z=(surf / 1e6).to_numpy(),
                x=[d.strftime("%Y-%m") for d in surf.index], y=list(surf.columns),
                colorscale="Viridis", colorbar=dict(title="Gross ($M)"))])
            fig_s.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                scene=dict(xaxis_title="Month", yaxis_title="Asset class",
                                           zaxis_title="Gross notional ($M)",
                                           camera=dict(eye=dict(x=1.5, y=-1.4, z=0.9))),
                                margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_s, width="stretch")

with tab_dq:
    st.subheader("Violations by severity and rule")
    if f_violations.empty:
        st.info("No DQ violations in the current filter.")
    else:
        left, right = st.columns(2)
        with left:
            sev = f_violations["severity"].value_counts().reset_index()
            sev.columns = ["severity", "violations"]
            st.plotly_chart(_style(px.bar(sev, x="severity", y="violations", color="severity",
                                          title="Violations by severity")), width="stretch")
        with right:
            by_rule = f_violations["rule_name"].value_counts().reset_index()
            by_rule.columns = ["rule_name", "violations"]
            st.plotly_chart(_style(px.bar(by_rule, x="violations", y="rule_name",
                                          orientation="h", title="Violations by rule")),
                            width="stretch")
        st.dataframe(f_violations, width="stretch")
        st.download_button("⬇️ Export violations (CSV)",
                           f_violations.to_csv(index=False).encode(),
                           file_name="dq_violations.csv", mime="text/csv")

with tab_lineage:
    st.subheader("Trace ancestry and downstream impact")
    try:
        from graph.queries import graph_summary, path_between, trace_ancestry, trace_impact

        summary = graph_summary(DUCKDB_PATH)
        m1, m2, m3 = st.columns(3)
        m1.metric("Nodes", summary.get("node_count", 0))
        m2.metric("Edges", summary.get("edge_count", 0))
        m3.metric("Transforms", len(summary.get("transforms", [])))
        st.write("Transforms:", ", ".join(summary.get("transforms", [])) or "None")
        target = st.text_input("Trace ancestry for column", value="risk_report.eur_exposure")
        if st.button("Trace Ancestry"):
            st.write(trace_ancestry(target, DUCKDB_PATH))
        source = st.text_input("Trace downstream impact from column", value="raw.notional")
        if st.button("Trace Impact"):
            st.write(trace_impact(source, DUCKDB_PATH))
        left, right = st.columns(2)
        ps = left.text_input("Path source", value="raw.notional")
        pt = right.text_input("Path target", value="risk_report.eur_exposure")
        if st.button("Find Path"):
            st.write(path_between(ps, pt, DUCKDB_PATH))
    except Exception as exc:  # graph helpers need the DuckDB file
        st.info(f"Lineage graph unavailable: {exc}")

with tab_runs:
    st.subheader("Pipeline stage runs")
    if runs.empty:
        st.info("No run history yet.")
    else:
        stage = (runs.groupby("stage_name", as_index=False)
                 .agg(records_in=("records_in", "sum"), records_out=("records_out", "sum"),
                      records_dead=("records_dead", "sum"), duration_sec=("duration_sec", "sum")))
        st.plotly_chart(_style(px.bar(stage, x="stage_name",
                                      y=["records_in", "records_out", "records_dead"],
                                      barmode="group", title="Records in / out / dead-letter by stage")),
                        width="stretch")
        st.dataframe(runs, width="stretch")
    st.subheader("Job runs")
    st.dataframe(query_table(
        """SELECT batch_id, job_name, status, started_at, finished_at, duration_sec,
                  attempt, exit_code, error FROM job_runs ORDER BY started_at DESC LIMIT 200"""),
        width="stretch")

st.caption("Use the sidebar pages for the full run history, DQ drilldown, lineage explorer and AI advisor.")
