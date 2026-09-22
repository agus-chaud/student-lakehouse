"""Adaptador de persistencia Gold para el orquestador."""

import pandas as pd

from gold import persist_gold


def run(
    cfg: dict,
    s3,
    gold_models: dict[str, pd.DataFrame],
) -> dict[str, dict[str, int]]:
    return persist_gold(s3, cfg, gold_models)
