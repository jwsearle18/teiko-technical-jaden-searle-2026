import sqlite3

import polars as pl
from scipy.stats import mannwhitneyu


DB_PATH = "data/teiko.db"


def _query(sql: str) -> pl.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pl.read_database(sql, conn)


def get_frequency_table() -> pl.DataFrame:
    """Compute relative frequency of each cell population per sample, with metadata."""
    df = _query("""
        SELECT cc.sample_id, cc.population, cc.count,
               sub.project_id, sub.condition, sub.treatment, s.sample_type
        FROM cell_counts cc
        JOIN samples s ON cc.sample_id = s.id
        JOIN subjects sub ON s.subject_id = sub.id
    """)

    total_counts = df.group_by("sample_id").agg(
        pl.col("count").sum().alias("total_count")
    )

    return (
        df.join(total_counts, on="sample_id")
        .with_columns(
            (pl.col("count") / pl.col("total_count") * 100)
            .round(2)
            .alias("percentage")
        )
        .select(
            pl.col("sample_id").alias("sample"),
            "project_id",
            "condition",
            "treatment",
            "sample_type",
            "total_count",
            "population",
            "count",
            "percentage",
        )
        .sort("sample", "population")
    )


def get_responder_frequencies() -> pl.DataFrame:
    """Cell population relative frequencies for melanoma/PBMC/miraclib patients."""
    df = _query("""
        SELECT cc.sample_id, cc.population, cc.count, sub.response
        FROM cell_counts cc
        JOIN samples s ON cc.sample_id = s.id
        JOIN subjects sub ON s.subject_id = sub.id
        WHERE sub.condition = 'melanoma'
          AND s.sample_type = 'PBMC'
          AND sub.treatment = 'miraclib'
    """)

    totals = df.group_by("sample_id").agg(
        pl.col("count").sum().alias("total_count")
    )

    return (
        df.join(totals, on="sample_id")
        .with_columns(
            (pl.col("count") / pl.col("total_count") * 100)
            .round(2)
            .alias("percentage")
        )
        .select("sample_id", "population", "percentage", "response")
        .sort("sample_id", "population")
    )


def get_significance_tests(df: pl.DataFrame) -> pl.DataFrame:
    """Mann-Whitney U test per population comparing responders vs non-responders."""
    rows = []
    for pop in df["population"].unique().sort().to_list():
        pop_df = df.filter(pl.col("population") == pop)
        responders = pop_df.filter(pl.col("response") == "yes")["percentage"].to_list()
        non_responders = pop_df.filter(pl.col("response") == "no")["percentage"].to_list()
        stat, p = mannwhitneyu(responders, non_responders, alternative="two-sided")
        rows.append({
            "population": pop,
            "u_statistic": round(stat, 2),
            "p_value": round(p, 4),
            "significant": p < 0.05,
        })
    return pl.DataFrame(rows).cast({"significant": pl.Boolean})


def get_baseline_samples() -> pl.DataFrame:
    """Melanoma PBMC baseline miraclib samples with subject metadata."""
    return _query("""
        SELECT s.id AS sample_id, sub.project_id, sub.id AS subject_id,
               sub.response, sub.sex
        FROM samples s
        JOIN subjects sub ON s.subject_id = sub.id
        WHERE sub.condition = 'melanoma'
          AND s.sample_type = 'PBMC'
          AND s.time_from_treatment_start = 0
          AND sub.treatment = 'miraclib'
    """)


def get_avg_bcell_male_responders() -> float:
    """Average B cell count for melanoma male responders at time=0."""
    df = _query("""
        SELECT cc.count
        FROM cell_counts cc
        JOIN samples s ON cc.sample_id = s.id
        JOIN subjects sub ON s.subject_id = sub.id
        WHERE sub.condition = 'melanoma'
          AND sub.sex = 'M'
          AND sub.response = 'yes'
          AND s.time_from_treatment_start = 0
          AND cc.population = 'b_cell'
    """)
    return round(df["count"].mean(), 2)
