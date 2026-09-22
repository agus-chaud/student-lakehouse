"""Adaptador de transformación Gold para el orquestador."""

import pandas as pd

from gold import transform_gold


def run(df_silver: pd.DataFrame, df_zones: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return transform_gold(df_silver, df_zones)
