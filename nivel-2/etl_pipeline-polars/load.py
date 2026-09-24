import logging
import re

import polars as pl
from sqlalchemy import create_engine


def build_connection_string(cfg: dict) -> str:
    pg = cfg["postgres"]
    return (
        f"postgresql://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['db']}"
    )


def load_dataframe_to_postgres(cfg: dict, table: str, df: pl.DataFrame) -> None:
    """Crea/reemplaza la tabla en la base `postgres` con el contenido del DataFrame."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"Nombre de tabla inválido: {table}")
    engine = create_engine(build_connection_string(cfg))
    try:
        # Los modelos Gold son pequeños: pasar a pandas solo para to_sql es barato.
        df.to_pandas().to_sql(table, engine, if_exists="replace", index=False)
    finally:
        engine.dispose()
    logging.getLogger("etl_pipeline").info(
        "Modelo Gold cargado en PostgreSQL | tabla=%s | filas=%s", table, df.height
    )
