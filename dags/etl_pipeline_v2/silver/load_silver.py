"""Persistencia de la capa Silver en MinIO."""

import io
import logging
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from etl_pipeline_v2.extract import ensure_bucket
from etl_pipeline_v2.utils import measure


logger = logging.getLogger("etl_pipeline.silver.load")


def run(cfg: dict, s3, frames: Iterable[pd.DataFrame]) -> dict[str, int]:
    """Escribe Silver en MinIO consumiendo los DataFrames de a uno.

    Cada frame (un row group ya transformado) se agrega a un ParquetWriter,
    de modo que nunca hay más de un row group de Silver en memoria.
    """
    destination = cfg["silver"]
    ensure_bucket(s3, destination["bucket"])
    buffer = io.BytesIO()
    writer = None
    total_rows = 0
    with measure("transform_load_silver") as metrics:
        for frame in frames:
            table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(buffer, table.schema)
            else:
                table = table.cast(writer.schema)
            writer.write_table(table)
            total_rows += len(frame)
            del frame, table
        if writer is not None:
            writer.close()
        s3.put_object(
            Bucket=destination["bucket"],
            Key=destination["object_path"],
            Body=buffer.getvalue(),
        )
        metrics.rows_processed = total_rows
    metrics_out = {"total_rows": total_rows}
    logger.info(
        "Silver persistido | bucket=%s | filas=%s",
        destination["bucket"],
        total_rows,
    )
    return metrics_out


def read_silver(cfg: dict, s3, columns: list[str] | None = None) -> pd.DataFrame:
    """Lee Silver desde MinIO, opcionalmente solo algunas columnas."""
    destination = cfg["silver"]
    body = s3.get_object(Bucket=destination["bucket"], Key=destination["object_path"])["Body"].read()
    available = set(pq.ParquetFile(io.BytesIO(body)).schema_arrow.names)
    if columns is not None:
        columns = [c for c in columns if c in available]
    return pq.read_table(io.BytesIO(body), columns=columns).to_pandas()
