import pandas as pd

from ec_poverty_monitor.sources.inec_poverty import _parse_timeseries_sheet


def test_parse_timeseries_sheet_poverty_incidence_month_end_and_carry_forward() -> None:
    # Mimic INEC sheet layout: header row with Período/Incidencia; rows carry forward period label.
    grid = pd.DataFrame(
        [
            [None, None, None, None],
            [None, "Período", None, "Incidencia (1)"],
            [None, "Junio", "2008", 34.97],
            [None, None, "2009 (2)", 33.01],
            [None, "Diciembre", 2008, 36.74],
        ]
    )

    rows = _parse_timeseries_sheet(grid=grid, sheet_name="1.1.1.pobre_nacional")
    # Includes national alias (no suffix) and national-suffixed.
    assert any(r["series"] == "poverty_rate_pct_national" for r in rows)
    assert any(r["series"] == "poverty_rate_pct" for r in rows)

    # Check that month-end dates are correct.
    dates = {(r["series"], r["period_date"], r["value"]) for r in rows}
    assert ("poverty_rate_pct", pd.Timestamp("2008-06-30").date(), 34.97) in dates
    assert ("poverty_rate_pct", pd.Timestamp("2009-06-30").date(), 33.01) in dates
    assert ("poverty_rate_pct", pd.Timestamp("2008-12-31").date(), 36.74) in dates
