import io
import sys
from time import perf_counter

import pandas as pd
import pyarrow.parquet as pq

from utils import load_config, setup_logging
import extract
from silver import load_silver, transform_silver
import transform_gold
import load_gold


logger = setup_logging()

def row_groups_pipeline(pqfile: pq.ParquetFile):
    """Expone los row groups de Bronze para consumidores compatibles."""
    total = pqfile.num_row_groups
    for index in range(total):
        yield index, total, pqfile.read_row_group(index).to_pandas()


def run_pipeline(config_path: str = "config.yml") -> None:
    t_global = perf_counter()
    logger.info("=" * 60)
    logger.info("Pipeline ETL iniciado — Bronze → Silver → Gold")
    logger.info("=" * 60)

    # CONFIG
    try:
        cfg = load_config(config_path)
        logger.info("Configuración cargada correctamente.")
    except (FileNotFoundError, EnvironmentError) as e:
        logger.error(f"Error de configuración: {e}")
        sys.exit(1)

    # El cliente S3 se construye una vez y se comparte entre todas las etapas.
    s3 = extract.build_s3_client(cfg)

    try:
        logger.info("── ETAPA 1: EXTRACT BRONZE — Viajes ──")
        pqfile = extract.run(cfg, s3=s3)
    except Exception as error:
        logger.error(f"Fallo en EXTRACT BRONZE (VIAJES): {error}", exc_info=True)
        sys.exit(1)

    try:
        logger.info("── ETAPA 2: EXTRACT BRONZE — Lookup ──")
        lookup_bytes = extract.run_lookup(cfg, s3=s3)
        lookup = extract.read_bronze_csv(
            s3,
            cfg["lookup"]["bucket"],
            cfg["lookup"]["object_path"],
        )
    except Exception as error:
        logger.error(f"Fallo en EXTRACT BRONZE (LOOKUP): {error}", exc_info=True)
        sys.exit(1)

    try:
        logger.info("── ETAPA 3: BRONZE — Validación ──")
        extract.validate_bronze(s3, cfg, pqfile, lookup)
    except Exception as error:
        logger.error(f"Fallo en BRONZE: {error}", exc_info=True)
        sys.exit(1)

    try:
        logger.info("── ETAPA 4: TRANSFORM + LOAD SILVER ──")
        bronze_frames = [
            pqfile.read_row_group(index).to_pandas()
            for index in range(pqfile.num_row_groups)
        ]
        df_bronze = pd.concat(bronze_frames, ignore_index=True)
        df_silver = transform_silver.run(df_bronze)
        silver_metrics = load_silver.run(cfg, s3, df_silver)
    except Exception as error:
        logger.error(f"Fallo en SILVER: {error}", exc_info=True)
        sys.exit(1)

    try:
        logger.info("── ETAPA 5: TRANSFORM + LOAD GOLD ──")
        df_zones = pd.read_csv(io.BytesIO(lookup_bytes))
        gold_models = transform_gold.run(df_silver, df_zones)
        gold_metrics = load_gold.run(cfg, s3, gold_models)
    except Exception as error:
        logger.error(f"Fallo en GOLD: {error}", exc_info=True)
        sys.exit(1)

    elapsed_total = perf_counter() - t_global
    logger.info("=" * 60)
    logger.info(
        "Pipeline finalizado exitosamente | silver_rows=%s | gold_models=%s | tiempo_total=%.2fs",
        silver_metrics["total_rows"],
        len(gold_metrics),
        elapsed_total,
    )
    for model_name, metrics in gold_metrics.items():
        logger.info("  %s: %s filas → PostgreSQL", model_name, metrics["rows"])
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL Pipeline — NYC Taxi Lakehouse")
    parser.add_argument("--config", default="config.yml")
    args = parser.parse_args()
    run_pipeline(config_path=args.config)
