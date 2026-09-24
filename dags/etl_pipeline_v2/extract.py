import io
import logging

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from etl_pipeline_v2.utils import measure

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
    """Crea el bucket si no existe. Idempotente y segura ante condiciones de carrera.

    No hay chequeo previo (list_buckets): si otra tarea crea el bucket entre
    medias, MinIO responde BucketAlreadyOwnedByYou / BucketAlreadyExists y
    se registra sin fallar. Cualquier otro error se propaga.
    """
    try:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' creado.")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            logger.info(f"Bucket '{bucket}' ya existe (creado por otra tarea).")
        else:
            raise


def upload_to_bronze(s3, bucket: str, object_path: str, data: bytes) -> None:
    """Sube el archivo crudo a la capa Bronze."""
    s3.put_object(Bucket=bucket, Key=object_path, Body=data)
    logger.info(f"Archivo almacenado en Bronze → {bucket}/{object_path}")


def already_in_bronze(s3, bucket: str, object_path: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=object_path)
        return True
    except s3.exceptions.ClientError:
        return False


def download_file(url: str) -> bytes:
    """Descarga un archivo desde una URL y devuelve sus bytes crudos."""
    logger.info(f"Descargando desde: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    logger.info(f"Descarga completa — {len(response.content) / (1024**2):.1f} MB")
    return response.content


# Aliases de compatibilidad con consumidores de Fase-2.
download_parquet = download_file
download_csv = download_file


def read_bronze_parquet(s3, bucket: str, object_path: str) -> pq.ParquetFile:
    obj = s3.get_object(Bucket=bucket, Key=object_path)
    return pq.ParquetFile(io.BytesIO(obj["Body"].read()))


def read_bronze_csv(s3, bucket: str, object_path: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=object_path)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def extract_trips(s3, cfg: dict) -> pq.ParquetFile:
    bucket = cfg["bronze"]["bucket"]
    obj_path = cfg["bronze"]["object_path"]

    ensure_bucket(s3, bucket)

    with measure("extract") as m:
        if already_in_bronze(s3, bucket, obj_path):
            logger.info("Parquet ya en Bronze. Saltando descarga.")
        else:
            raw_bytes = download_file(cfg["source"]["url"])
            upload_to_bronze(s3, bucket, obj_path, raw_bytes)

        pqfile = read_bronze_parquet(s3, bucket, obj_path)

    m.extra = {"num_row_groups": pqfile.num_row_groups}
    logger.info(f"ParquetFile listo — {pqfile.num_row_groups} row groups")

    return pqfile


def extract_lookup(s3, cfg: dict) -> pd.DataFrame:
    lookup = cfg["lookup"]
    ensure_bucket(s3, lookup["bucket"])
    if not already_in_bronze(s3, lookup["bucket"], lookup["object_path"]):
        upload_to_bronze(
            s3,
            lookup["bucket"],
            lookup["object_path"],
            download_csv(lookup["url"]),
        )
    return read_bronze_csv(s3, lookup["bucket"], lookup["object_path"])


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
            raw_bytes = download_file(lookup["url"])
            upload_to_bronze(s3, bucket, obj_path, raw_bytes)

        obj = s3.get_object(Bucket=bucket, Key=obj_path)
        csv_bytes = obj["Body"].read()

    metrics.extra = {"size_kb": round(len(csv_bytes) / 1024, 1)}
    logger.info(f"Lookup listo — {len(csv_bytes) / 1024:.1f} KB")
    return csv_bytes


def validate_bronze(s3, cfg: dict, pqfile: pq.ParquetFile, lookup: pd.DataFrame) -> None:
    if pqfile.num_row_groups == 0:
        raise ValueError("El Parquet de viajes en Bronze no contiene row groups")
    if lookup.empty:
        logging.getLogger("etl_pipeline").warning("El lookup en Bronze está vacío")
    logging.getLogger("etl_pipeline").info(
        "Bronze validado | viajes_row_groups=%s | lookup_filas=%s",
        pqfile.num_row_groups,
        len(lookup),
    )


def run(cfg: dict, s3=None) -> pq.ParquetFile:
    """Extrae viajes usando un cliente S3 compartido cuando se proporciona."""
    return extract_trips(s3 if s3 is not None else build_s3_client(cfg), cfg)
