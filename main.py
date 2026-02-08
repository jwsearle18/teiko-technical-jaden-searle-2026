from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

from analysis import (
    DB_PATH,
    get_avg_bcell_male_responders,
    get_baseline_samples,
    get_frequency_table,
    get_responder_frequencies,
    get_significance_tests,
)
from database import init_db, load_csv

st.set_page_config(page_title="Teiko Clinical Trial", layout="wide")

if not Path(DB_PATH).exists():
    engine = init_db(DB_PATH)
    load_csv(engine, "data/cell-count.csv")

st.title("Teiko Clinical Trial Analysis")

tab1, tab2, tab3 = st.tabs(["Data Overview", "Statistical Analysis", "Subset Analysis"])

# ── Part 2: Data Overview ──
with tab1:
    freq_df = get_frequency_table()

    # Filters
    f1, f2, f3, f4 = st.columns(4)
    projects = ["All"] + sorted(freq_df["project_id"].unique().to_list())
    conditions = ["All"] + sorted(freq_df["condition"].unique().to_list())
    treatments = ["All"] + sorted(freq_df["treatment"].unique().to_list())
    sample_types = ["All"] + sorted(freq_df["sample_type"].unique().to_list())

    sel_project = f1.selectbox("Project", projects)
    sel_condition = f2.selectbox("Condition", conditions)
    sel_treatment = f3.selectbox("Treatment", treatments)
    sel_sample_type = f4.selectbox("Sample Type", sample_types)

    filtered = freq_df
    if sel_project != "All":
        filtered = filtered.filter(pl.col("project_id") == sel_project)
    if sel_condition != "All":
        filtered = filtered.filter(pl.col("condition") == sel_condition)
    if sel_treatment != "All":
        filtered = filtered.filter(pl.col("treatment") == sel_treatment)
    if sel_sample_type != "All":
        filtered = filtered.filter(pl.col("sample_type") == sel_sample_type)

    m1, m2 = st.columns(2)
    m1.metric("Samples", filtered["sample"].n_unique())
    m2.metric("Records", f"{len(filtered):,}")

    t1, t2 = st.columns([2, 1])
    sample_search = t1.text_input("Search samples", placeholder="e.g. sample00000")
    all_pops = sorted(filtered["population"].unique().to_list())
    sel_pops = t2.multiselect("Populations", all_pops, default=all_pops)

    display_df = filtered.select("sample", "total_count", "population", "count", "percentage")
    if sample_search:
        display_df = display_df.filter(pl.col("sample").str.contains(sample_search))
    if sel_pops:
        display_df = display_df.filter(pl.col("population").is_in(sel_pops))

    st.dataframe(
        display_df.to_pandas(),
        use_container_width=True,
        hide_index=True,
        column_config={
            "sample": "Sample",
            "total_count": st.column_config.NumberColumn("Total Count", format="%d"),
            "population": "Population",
            "count": st.column_config.NumberColumn("Count", format="%d"),
            "percentage": st.column_config.NumberColumn("Frequency (%)", format="%.2f%%"),
        },
    )

# ── Part 3: Statistical Analysis ──
with tab2:
    st.caption("Melanoma PBMC miraclib — Responders vs Non-Responders")

    resp_df = get_responder_frequencies()

    fig = px.box(
        resp_df.to_pandas(),
        x="population",
        y="percentage",
        color="response",
        color_discrete_map={"yes": "#4A90D9", "no": "#E07B7B"},
        labels={
            "percentage": "Relative Frequency (%)",
            "population": "",
            "response": "Responder",
        },
        category_orders={
            "population": sorted(resp_df["population"].unique().to_list()),
        },
    )
    fig.update_layout(
        height=500,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5,
            title_text="",
        ),
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis=dict(
            ticktext=[p.replace("_", " ").title() for p in sorted(resp_df["population"].unique().to_list())],
            tickvals=sorted(resp_df["population"].unique().to_list()),
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

    sig_df = get_significance_tests(resp_df)

    st.subheader("Significance Tests")
    st.caption("Mann-Whitney U test (two-sided, α = 0.05)")

    sig_cols = st.columns(len(sig_df))
    for i, row in enumerate(sig_df.iter_rows(named=True)):
        label = row["population"].replace("_", " ").title()
        p_str = f"p = {row['p_value']:.4f}"
        sig_cols[i].metric(
            label,
            p_str,
            delta="Significant" if row["significant"] else "Not significant",
            delta_color="normal" if row["significant"] else "off",
        )

    with st.expander("View full test results"):
        st.dataframe(
            sig_df.to_pandas(),
            use_container_width=True,
            hide_index=True,
            column_config={
                "population": "Population",
                "u_statistic": st.column_config.NumberColumn("U Statistic", format="%.1f"),
                "p_value": st.column_config.NumberColumn("p-value", format="%.4f"),
                "significant": st.column_config.CheckboxColumn("Significant"),
            },
        )

# ── Part 4: Subset Analysis ──
with tab3:
    st.caption("Melanoma PBMC baseline (time = 0) patients treated with miraclib")

    baseline = get_baseline_samples()
    subjects = baseline.select("subject_id", "response", "sex").unique()

    project_counts = baseline.group_by("project_id").agg(
        pl.col("sample_id").count().alias("n")
    ).sort("project_id")

    st.subheader("Sample Count")
    proj_cols = st.columns(len(project_counts))
    for i, row in enumerate(project_counts.iter_rows(named=True)):
        proj_cols[i].metric(row["project_id"], row["n"])

    st.divider()

    response_counts = subjects.group_by("response").agg(pl.col("subject_id").count().alias("n")).sort("response")
    sex_counts = subjects.group_by("sex").agg(pl.col("subject_id").count().alias("n")).sort("sex")
    resp_map = dict(zip(response_counts["response"].to_list(), response_counts["n"].to_list()))
    sex_map = dict(zip(sex_counts["sex"].to_list(), sex_counts["n"].to_list()))

    st.subheader("Subject Count")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Responders", resp_map.get("yes", 0))
    c2.metric("Non-Responders", resp_map.get("no", 0))
    c3.metric("Male", sex_map.get("M", 0))
    c4.metric("Female", sex_map.get("F", 0))

    st.divider()

    avg_bcell = get_avg_bcell_male_responders()
    st.metric("Avg B Cell Count — Male Responders at Baseline", f"{avg_bcell:,.2f}")
