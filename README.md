# Teiko Clinical Trial Analysis

Interactive dashboard for analyzing immune cell population data from clinical trial patients.

**[Live Dashboard](https://teiko.streamlit.app)**

## Setup & Running

```bash
# Install dependencies (requires uv: https://docs.astral.sh/uv/)
uv sync

# Run the dashboard
uv run streamlit run main.py
```

The database is created automatically on first run from `data/cell-count.csv`.

### GitHub Codespaces

After opening the Codespace:

```bash
pip install uv
uv sync
uv run streamlit run main.py
```

Codespaces will prompt you to open the forwarded port in browser.

## Database Schema

```
projects
  └─ id (PK)

subjects
  ├─ id (PK)
  ├─ project_id (FK → projects)
  ├─ condition, age, sex
  └─ treatment, response

samples
  ├─ id (PK)
  ├─ subject_id (FK → subjects)
  ├─ sample_type
  └─ time_from_treatment_start

cell_counts
  ├─ id (PK, autoincrement)
  ├─ sample_id (FK → samples)
  ├─ population
  └─ count
```

The CSV is denormalized — each row mixes project, subject, sample, and cell count data. The schema normalizes this into four tables connected by foreign keys.

**Why this design:**

- **Avoids redundancy** — subject metadata (age, sex, treatment) is stored once per subject, not repeated across every sample row. Same for project-level data.
- **Flexible querying** — cell counts are stored in long format (one row per population per sample) rather than wide format (one column per population). This makes it straightforward to filter, group, and aggregate by any population without hardcoding column names.
- **Scales naturally** — adding hundreds of projects or thousands of samples doesn't require schema changes. New cell populations can be added as rows, not columns. Analytical queries (e.g., "compare B cell frequencies across all melanoma patients") remain simple JOINs regardless of dataset size.

## Code Structure

```
main.py        — Streamlit dashboard (UI and visualization)
analysis.py    — Data queries and statistical analysis
database.py    — SQLAlchemy schema and CSV loading
data/          — Source CSV and generated SQLite database
```

**`database.py`** defines the ORM models and handles loading the CSV into SQLite. SQLAlchemy was chosen for schema definition since it makes the relational design explicit and handles table creation.

**`analysis.py`** contains all data retrieval and computation. Each function runs a SQL query and returns a Polars DataFrame. This keeps the analysis logic separate from the UI — the functions can be called from scripts, notebooks, or the dashboard.

**`main.py`** is the Streamlit app. It calls the analysis functions and handles layout, filtering, and visualization with Plotly. The dashboard auto-initializes the database on first run so it's self-contained.
