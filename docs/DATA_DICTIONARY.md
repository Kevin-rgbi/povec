# Data dictionary (marts)

## Common columns

All mart tables include:

- `period_date` (date): canonical quarter-end date
- `period_grain` (string): `month|quarter|semiannual|year` (original source grain)
- `series` (string): internal series key
- `value` (float)
- `unit` (string): `%|USD|index|ratio`
- `source` (string): `INEC|WDI|PIP|...`
- `source_url` (string)
- `comparability_break` (bool): true if period intersects ENEMDU break window

## Tables

- `mart_indicators.parquet`: long-form indicator table used by the dashboard
- `mart_sources.parquet`: run-time source metadata (download URLs, checksums, timestamps)
