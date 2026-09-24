"""Adaptador de transformación Gold para el orquestador."""

import polars as pl

from gold import GOLD_INPUT_COLUMNS, transform_gold


def run(df_silver: pl.DataFrame, df_zones: pl.DataFrame) -> dict[str, pl.DataFrame]:
    return transform_gold(df_silver, df_zones)
