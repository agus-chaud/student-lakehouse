"""Persistencia de la capa Silver en MinIO."""

import io
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from extract import ensure_bucket


logger = logging.getLogger("etl_pipeline.silver.load")


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buffer)
    return buffer.getvalue()


def run(cfg: dict, s3, df_silver: pd.DataFrame) -> dict[str, int]:
    """Guarda Silver en MinIO y devuelve sus métricas básicas."""
    destination = cfg["silver"]
    ensure_bucket(s3, destination["bucket"])
    s3.put_object(
        Bucket=destination["bucket"],
        Key=destination["object_path"],
        Body=_parquet_bytes(df_silver),
    )
    metrics = {"total_rows": len(df_silver)}
    logger.info(
        "Silver persistido | bucket=%s | filas=%s",
        destination["bucket"],
        metrics["total_rows"],
    )
    return metrics