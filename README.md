# Ecuador Economy & Poverty Monitor (Option A: published series only)

A reproducible, end-to-end pipeline + dashboard that tracks Ecuador’s poverty, inequality, and labor-market indicators using **officially published series only** (no microdata ingestion).

## What this project does

- Downloads and standardizes **published tabulations** from INEC (Ecuador en Cifras)
- Pulls macro context from World Bank WDI
- Pulls inequality / poverty statistics from World Bank PIP
- Produces marts as **Parquet** + a **DuckDB** database
- Serves a **Streamlit dashboard** with clear metadata and comparability warnings

## Critical comparability note (ENEMDU)

INEC warns that **2020 through May 2021** includes methodological changes that affect historical comparability. This repo:

- Tags observations in that window as `comparability_break = true`
- Draws a vertical marker in charts
- Shows a warning message on relevant views

This is descriptive monitoring only (no causal claims).

## Quickstart

### 1) Install

Using Poetry:

```bash
poetry install
```

If `poetry run ecmon ...` fails with `ModuleNotFoundError: ec_poverty_monitor` on macOS + Python 3.13,
your virtualenv may contain `.pth` files marked with the `hidden` flag (Python skips “hidden” `.pth`).
Clearing the flag usually fixes it:

```bash
find .venv/lib/python3.*/site-packages -name "*.pth" -exec chflags 0 {} +
```

### 2) Run the pipeline

```bash
poetry run ecmon --config config/config.yaml
```

Artifacts:

- `data/raw/` downloaded source files
- `data/mart/` Parquet marts + `ecuador_monitor.duckdb`
- `runs/` run manifests (inputs, checksums, row counts)

### 3) Launch dashboard

```bash
poetry run ecmon dashboard --duckdb-path data/mart/ecuador_monitor.duckdb
```

Alternative (direct Streamlit):

```bash
poetry run streamlit run src/ec_poverty_monitor/dashboard/app.py
```

## Deploy (Streamlit Community Cloud)

This repo uses a `src/` layout, so Streamlit Cloud should run the root entrypoint:

- Main file path: `app.py`

Notes:

- The dashboard expects `data/mart/ecuador_monitor.duckdb` to exist.
- On Streamlit Cloud you can have the app build the marts on first load by setting:
	- `ECMON_AUTOBUILD=1`
	- (optional) `ECMON_CONFIG_PATH=config/config.yaml`

Dependencies:

- Streamlit Cloud installs from `requirements.txt`.

## Data sources (official)

- INEC Ecuador en Cifras (ENEMDU employment posts + tabulados)
- INEC Ecuador en Cifras (Pobreza por ingresos results posts + tabulados)
- World Bank WDI API
- World Bank PIP API

BCE and CEPALSTAT are supported as optional manual sources if you want to add cross-checks later.

## License

MIT
