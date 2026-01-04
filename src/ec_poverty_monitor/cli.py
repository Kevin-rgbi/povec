from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import typer

from ec_poverty_monitor.pipeline import run_pipeline

app = typer.Typer(add_completion=False, invoke_without_command=True)


@app.callback()
def main(
    ctx: typer.Context,
    config: Path = typer.Option(None, exists=True, dir_okay=False, help="Path to config YAML"),
    force: bool = typer.Option(
        False,
        "--force",
        help="Redownload even if cached",
        is_flag=True,
    ),
) -> None:
    """Run the full pipeline: ingest -> standardize -> marts -> DuckDB."""
    if ctx.invoked_subcommand is not None:
        return
    if config is None:
        raise typer.BadParameter("--config is required")
    result = run_pipeline(config_path=config, force=force)
    typer.echo(json.dumps(result, indent=2, ensure_ascii=False))


@app.command()
def run(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to config YAML"),
    force: bool = typer.Option(
        False,
        "--force",
        help="Redownload even if cached",
        is_flag=True,
    ),
) -> None:
    """Run the pipeline (compat command)."""
    result = run_pipeline(config_path=config, force=force)
    typer.echo(json.dumps(result, indent=2, ensure_ascii=False))


@app.command()
def dashboard(
    duckdb_path: Path = typer.Option(
        Path("data/mart/ecuador_monitor.duckdb"),
        help="Path to DuckDB file to use as dashboard default",
    ),
) -> None:
    """Launch the Streamlit dashboard."""
    env = os.environ.copy()
    env["ECMON_DUCKDB_PATH"] = str(duckdb_path)

    dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
    raise typer.Exit(
        subprocess.call(
            [sys.executable, "-m", "streamlit", "run", str(dashboard_path)],
            env=env,
        )
    )


if __name__ == "__main__":
    app()
