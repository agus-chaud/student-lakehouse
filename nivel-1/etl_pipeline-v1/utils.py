"""
utils.py — Logging, métricas y helpers compartidos

Responsabilidad única:
  - Configurar logging estructurado
  - Registrar métricas de tiempo y memoria por etapa
  - Proveer helpers reutilizables

NO contiene lógica de negocio.
NO sabe nada de MinIO, Postgres ni Parquet.
"""

import os
import time
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

import yaml


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configura logging estructurado con timestamp y nivel."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("etl_pipeline")


logger = setup_logging()


# ─────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────

@dataclass
class StageMetrics:
    """Métricas de una etapa del pipeline."""
    stage: str
    elapsed_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    rows_processed: int = 0
    extra: dict = field(default_factory=dict)

    def log(self):
        logger.info(
            f"[METRICS] stage={self.stage} | "
            f"time={self.elapsed_seconds:.2f}s | "
            f"peak_mem={self.peak_memory_mb:.1f}MB | "
            f"rows={self.rows_processed:,}"
            + (f" | {self.extra}" if self.extra else "")
        )


def _rss_mb() -> float:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / (1024 ** 2)


@contextmanager
def measure(stage: str, rows: int = 0, extra: Optional[dict] = None):
    """
    Mide tiempo y memoria (RSS del proceso) de una etapa.

    peak_mem = máximo RSS observado durante la etapa (muestreo cada 20 ms).
    Incluye memoria de pandas/pyarrow, que tracemalloc no ve. Es el uso
    total del proceso, no solo lo que asigna la etapa; se agrega
    extra["rss_delta_mb"] con lo que creció respecto al inicio.
    """
    rss_start = _rss_mb()
    peak = [rss_start]
    stop = threading.Event()

    def _sample():
        while not stop.wait(0.02):
            peak[0] = max(peak[0], _rss_mb())

    sampler = threading.Thread(target=_sample, daemon=True)
    sampler.start()

    t_start = time.perf_counter()
    metrics = StageMetrics(stage=stage, rows_processed=rows, extra=extra or {})
    try:
        yield metrics
    finally:
        stop.set()
        sampler.join()
        metrics.elapsed_seconds = time.perf_counter() - t_start
        peak[0] = max(peak[0], _rss_mb())
        metrics.peak_memory_mb = peak[0]
        metrics.extra["rss_delta_mb"] = round(peak[0] - rss_start, 1)
        metrics.rows_processed = rows or metrics.rows_processed
        metrics.log()


# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

def load_config(path: str = "config.yml") -> dict:
    """Carga config.yml y resuelve variables de entorno para credenciales."""
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    cfg["minio"]["access_key"] = _require_env("MINIO_ROOT_USER")
    cfg["minio"]["secret_key"] = _require_env("MINIO_ROOT_PASSWORD")
    cfg["postgres"]["password"] = _require_env("POSTGRES_PASSWORD")

    return cfg


def _require_env(var: str) -> str:
    """Lee una variable de entorno obligatoria. Falla claro si falta."""
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(
            f"Variable de entorno requerida no encontrada: '{var}'\n"
            f"Asegúrate de exportarla antes de correr el pipeline."
        )
    return value
