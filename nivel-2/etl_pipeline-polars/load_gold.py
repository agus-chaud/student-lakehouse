"""Adaptador de persistencia Gold para el orquestador."""

import polars as pl

from gold import persist_gold


def run(
    cfg: dict,
    s3,
    gold_models: dict[str, pl.DataFrame],
) -> dict[str, dict[str, int]]:
    return persist_gold(s3, cfg, gold_models)
