# Estado del proyecto — Student Lakehouse

*Actualizado: 2026-09-24*

## 1. ¿Qué es este proyecto?

Un mini "lakehouse" de práctica con los viajes de taxis amarillos de Nueva York (enero 2024). Toma un archivo crudo de internet, lo limpia por pasos y termina con tablas listas para analizar.

Se organiza en **tres capas**, como una línea de producción:

```
   INTERNET                MinIO (S3)                     MinIO + PostgreSQL
  ┌────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────────────────┐
  │ Parquet│──►│  BRONZE  │──►│  SILVER  │──►│           GOLD           │
  │  + CSV │   │  crudo   │   │  limpio  │   │ 6 tablas ya agregadas    │
  └────────┘   └──────────┘   └──────────┘   └──────────────────────────┘
```

| Capa | Qué contiene | Dónde queda |
|---|---|---|
| **Bronze** | Los archivos tal como se descargan (viajes + tabla de zonas) | MinIO, bucket `bronze` |
| **Silver** | Los viajes sin datos inválidos y con columnas extra (duración, velocidad, % de propina…) | MinIO, bucket `silver` |
| **Gold** | Resúmenes para responder preguntas de negocio | MinIO, bucket `gold`, y tablas en PostgreSQL |

## 2. Dónde estamos hoy

**El pipeline completo funciona de punta a punta, ejecutado desde Airflow.** Se probó el 2026-09-23: las tres tareas terminaron bien y los datos aparecieron en MinIO y en PostgreSQL.

### Resultados de la última ejecución

| Etapa | Tiempo | Resultado |
|---|---|---|
| `extract_bronze` | ~3 s | 3 grupos de filas de viajes y 265 zonas, validados |
| `process_silver` | ~32 s | 2.724.160 viajes limpios (se descartaron 240.464 por datos inválidos) |
| `process_gold` | ~26 s | 6 tablas generadas |

### Las 6 tablas Gold

| Tabla | Filas | Para qué sirve |
|---|---|---|
| `gold_hourly_demand` | 749 | Cuántos viajes hay por hora |
| `gold_zone_performance` | 253 | Rendimiento de cada zona |
| `gold_tip_analysis` | 4 | Análisis de propinas |
| `gold_daily_summary` | 35 | Resumen por día |
| `gold_revenue_by_payment` | 4 | Ingresos por forma de pago |
| `gold_route_analysis` | 23.259 | Rutas más comunes (origen → destino) |

## 3. El cambio más reciente: el DAG en tres tareas

**Antes:** el DAG `etl_pipeline_v2` tenía **una sola tarea** que hacía todo. Si algo fallaba en Gold, había que repetir también Bronze y Silver.

**Ahora:** el DAG nuevo `etl_pipeline` tiene **tres tareas independientes**:

```
extract_bronze  ──►  process_silver  ──►  process_gold
```

Cada una puede fallar, reintentarse o ejecutarse sola. Se comprobó: al reejecutar solo `process_gold`, Bronze y Silver no se volvieron a correr.

### ¿Cómo se logró?

Antes las etapas se pasaban los datos "de mano en mano" dentro de la memoria del programa. Airflow corre cada tarea en un proceso distinto, así que eso no funciona. La solución fue que **cada etapa guarde su resultado en MinIO y la siguiente lo lea desde ahí**. Así ninguna depende de que la anterior siga "viva".

Últimos commits: modelo Gold `route_analysis`, y `transform_gold` ahora filtra solo las columnas necesarias y mide memoria en `measure`.

Otros cambios en [main.py](dags/etl_pipeline_v2/main.py):

- `run_pipeline(config, stages=[...])` permite elegir qué etapas correr: `extract`, `silver` o `gold`. Sin `stages`, corre todas.
- Los errores ya no cortan el programa con `sys.exit`; se propagan para que Airflow marque la tarea en rojo y aplique el reintento.

También se puede correr desde la terminal:

```bash
python etl_pipeline_v2/main.py --stages silver gold
```

## 4. Mapa de carpetas

| Ruta | Contenido |
|---|---|
| `dags/dag_etl_pipeline.py` | **DAG nuevo** (3 tareas, `@daily`) |
| `dags/dag_etl_pipeline_v2.py` | DAG anterior (1 tarea, manual) |
| `dags/etl_pipeline_v2/` | Código del pipeline: `main.py` (orquestador), `extract.py`, `silver/`, `gold.py`, `utils.py`, `config.yml` |
| `nivel-0/`, `nivel-1/` | Ejercicios anteriores (notebooks y scripts exploratorios) |
| `nivel-2/pandas_vs_polars/` | **Nuevo:** benchmarks de la transformación Silver con pandas vs polars (`bench_silver_transform_*.py`, `requirements.txt`) |
| `config/` | Variables de entorno de los servicios |
| `docker-compose.yml` | Airflow, PostgreSQL, MinIO, Redis y demás servicios |

## 5. Cómo ejecutarlo

1. Levantar los servicios: `docker compose up -d` (para el pipeline bastan Airflow, MinIO, PostgreSQL y Redis).
2. Abrir Airflow en `http://localhost:7777`.
3. Activar el DAG `etl_pipeline` y dispararlo.

Las credenciales (MinIO y PostgreSQL) se leen de variables de entorno; si falta alguna, el pipeline falla con un mensaje claro.

## 6. Pendientes y cosas a tener en cuenta

- **`dags/` está en `.gitignore`** (línea 38). Los cambios en el pipeline y los DAGs **no aparecen en `git status`** y un commit no los incluiría. Hay que decidir si se versiona esa carpeta.
- **Falta probar un fallo real** (por ejemplo, apagar MinIO a mitad de ejecución) para confirmar que el reintento automático (`retries: 1`) funciona. Solo se probó el caso normal y el reintento manual de una tarea.
- **Cada etapa depende de que la anterior haya corrido al menos una vez.** Si Silver no existe en MinIO, Gold falla. Es lo esperado.
- **Solo se procesa enero 2024.** La URL está fija en `config.yml`, aunque el DAG corre `@daily`.
- **Cambios sin confirmar en git:** modificados `.devcontainer/devcontainer.json`, `.gitignore` (ahora ignora `DE.docx`), `docker-compose.yml`; sin seguimiento: `DE_backup_2026-09-23.docx`, `ESTADO_PROYECTO.md` y `nivel-2/`.
- **Benchmark pandas vs polars sin ejecutar/documentar:** los scripts de `nivel-2/` existen pero aún no hay resultados registrados aquí.
- **El DAG viejo `etl_pipeline_v2`** sigue existiendo y convive con el nuevo. Se puede retirar cuando ya no haga falta.
