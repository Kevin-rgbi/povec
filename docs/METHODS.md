# Methods

## Scope and constraints

- **Option A only**: this project ingests **published series/tabulations** only.
- It explicitly avoids microdata ingestion (e.g., INEC “BDD”, “SPSS”, “DATOS ABIERTOS” files).
- It produces descriptive monitoring outputs only.

## Canonical time grain

The canonical grain is **quarterly**:

- Monthly labor indicators (ENEMDU “Empleo …” posts) are converted to quarters via a simple quarter assignment (month -> quarter).
- Semiannual poverty releases (June/December) are assigned to Q2/Q4 respectively.
- Annual PIP/WDI series are assigned to Q4 (year-end).

This keeps joins simple and makes the dashboard readable while preserving source periodicity in metadata.

## ENEMDU comparability break (2020–May 2021)

INEC notes that from 2020 through May 2021 there were methodological changes affecting historical comparability.

The pipeline encodes this as:

- `comparability_break = true` for any period intersecting 2020-01-01 through 2021-05-31
- Dashboard marker and a visible warning

## Data sources

- INEC Ecuador en Cifras (WordPress API discovery of official posts; direct `documentos/web-inec/...` assets)
- World Bank WDI API
- World Bank PIP API

BCE and CEPALSTAT are not required for the core pipeline and can be added manually if needed.
