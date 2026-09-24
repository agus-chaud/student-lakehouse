import io
import logging

import boto3
import polars as pl
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError

from utils import measure

logger = logging.getLogger("etl_pipeline.extract")


def build_s3_client(cfg: dict) -> boto3.client:
    """Construye y devuelve el cliente S3/MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=cfg["minio"]["endpoint"],
        aws_access_key_id=cfg["minio"]["access_key"],
        aws_secret_access_key=cfg["minio"]["secret_key"],
    )


def ensure_bucket(s3, bucket: str) -> None:
    """Crea el bucket si no existe. Idempotente: las re-ejecuciones no rompen el lab.

    Sin chequeo previo: se intenta crear y, si ya existe, se ignora el error típico
    (BucketAlreadyOwnedByYou / BucketAlreadyExists). Cualquier otro error se propaga.
    """
    try:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' creado.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    except s3.exceptions.BucketAlreadyExists:
        pass


def upload_to_bronze(s3, bucket: str, object_path: str, data: bytes) -> None:
    """Sube el archivo crudo a la capa Bronze."""
    s3.put_object(Bucket=bucket, Key=object_path, Body=data)
    logger.info(f"Archivo almacenado en Bronze → {bucket}/{object_path}")


def already_in_bronze(s3, bucket: str, object_path: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=object_path)
        return True
    except ClientError:
        return False


def download_file(url: str) -> bytes:
    """Descarga un archivo desde una URL y devuelve sus bytes crudos."""
    logger.info(f"Descargando desde: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    logger.info(f"Descarga completa — {len(response.content) / (1024**2):.1f} MB")
    return response.content


def read_bronze_parquet(s3, bucket: str, object_path: str) -> pq.ParquetFile:
    obj = s3.get_object(Bucket=bucket, Key=object_path)
    return pq.ParquetFile(io.BytesIO(obj["Body"].read()))


def read_bronze_csv(s3, bucket: str, object_path: str) -> pl.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=object_path)
    return pl.read_csv(io.BytesIO(obj["Body"].read()))


def extract_trips(s3, cfg: dict) -> pq.ParquetFile:
    bucket = cfg["bronze"]["bucket"]
    obj_path = cfg["bronze"]["object_path"]

    ensure_bucket(s3, bucket)

    with measure("extract") as m:
        if already_in_bronze(s3, bucket, obj_path):
            logger.info("Parquet ya en Bronze. Saltando descarga.")
        else:
            upload_to_bronze(s3, bucket, obj_path, download_file(cfg["source"]["url"]))

        pqfile = read_bronze_parquet(s3, bucket, obj_path)

    m.extra = {"num_row_groups": pqfile.num_row_groups}
    logger.info(f"ParquetFile listo — {pqfile.num_row_groups} row groups")
    return pqfile


def run_lookup(cfg: dict, s3=None) -> bytes:
    """Descarga y persiste taxi_zone_lookup.csv en Bronze."""
    if s3 is None:
        s3 = build_s3_client(cfg)

    lookup = cfg["lookup"]
    bucket = lookup["bucket"]
    obj_path = lookup["object_path"]

    ensure_bucket(s3, bucket)

    with measure("extract_lookup") as metrics:
        if already_in_bronze(s3, bucket, obj_path):
            logger.info("Lookup ya en Bronze. Saltando descarga.")
        else:
            upload_to_bronze(s3, bucket, obj_path, download_file(lookup["url"]))

        csv_bytes = s3.get_object(Bucket=bucket, Key=obj_path)["Body"].read()

    metrics.extra = {"size_kb": round(len(csv_bytes) / 1024, 1)}
    logger.info(f"Lookup listo — {len(csv_bytes) / 1024:.1f} KB")
    return csv_bytes


def validate_bronze(s3, cfg: dict, pqfile: pq.ParquetFile, lookup: pl.DataFrame) -> None:
    if pqfile.num_row_groups == 0:
        raise ValueError("El Parquet de viajes en Bronze no contiene row groups")
    if lookup.is_empty():
        logger.warning("El lookup en Bronze está vacío")
    logger.info(
        "Bronze validado | viajes_row_groups=%s | lookup_filas=%s",
        pqfile.num_row_groups,
        lookup.height,
    )


def run(cfg: dict, s3=None) -> pq.ParquetFile:
    """Extrae viajes usando un cliente S3 compartido cuando se proporciona."""
    return extract_trips(s3 if s3 is not None else build_s3_client(cfg), cfg)
