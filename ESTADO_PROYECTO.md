# Estado del proyecto — Student Lakehouse

*Actualizado: 2026-09-24 (versión Polars en nivel-2)*

## 1. ¿Qué es este proyecto?

Un mini "lakehouse" de práctica con los viajes de taxis de Nueva York (enero 2024). Agarra un archivo crudo de internet, lo limpia por pasos y termina con tablas listas para analizar.

Se organiza en **tres capas**:

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
| **Silver** | Los viajes sin datos inválidos y con columnas calculadas (duración, velocidad, % de propina…) | MinIO, bucket `silver` |
| **Gold** | Resúmenes para responder preguntas de negocio | MinIO, bucket `gold`, y tablas en PostgreSQL |

## 2. Dónde estamos hoy

**El pipeline completo funciona de punta a punta, ejecutado desde Airflow.** Se probó el 2026-09-23: las tres tareas terminaron bien y los datos aparecieron en MinIO y en PostgreSQL.

### Resultados de la última ejecución

| Etapa            | Tiempo| Resultado |
|---               |---    |---|
| `extract_bronze` | ~3 s  | 3 grupos de filas de viajes y 265 zonas, validados |
| `process_silver` | ~32 s | 2.724.160 viajes limpios (se descartaron 240.464 por datos inválidos) |
| `process_gold`   | ~26 s | 6 tablas generadas |

### Las 6 tablas Gold

| Tabla | Filas | Para qué sirve |
|---|---|---|
| `gold_hourly_demand`     | 749 | Cuántos viajes hay por hora |
| `gold_zone_performance`  | 253 | Rendimiento de cada zona |
| `gold_tip_analysis`      | 4 | Análisis de propinas |
| `gold_daily_summary`     | 35 | Resumen por día |
| `gold_revenue_by_payment`| 4 | Ingresos por forma de pago |
| `gold_route_analysis`    | 23.259 | Rutas más comunes (origen → destino) |

## 3. El DAG en tres tareas

**Antes:** el DAG `etl_pipeline_v2` tenía **una sola tarea** que hacía todo. Si algo fallaba en Gold, había que repetir también Bronze y Silver.

**Ahora:** el DAG nuevo `etl_pipeline` tiene **tres tareas independientes**:

```
extract_bronze  ──►  process_silver  ──►  process_gold
```

Cada una puede fallar, reintentarse o ejecutarse sola. 

### ¿Cómo se logró?

Antes las etapas se pasaban los datos "de mano en mano" dentro de la memoria del programa. Airflow corre cada tarea en un proceso distinto, así que eso no funciona. La solución fue que **cada etapa guarde su resultado en MinIO y la siguiente lo lea desde ahí**. Así ninguna depende de que la anterior siga "viva".

Últimos commits: modelo Gold `route_analysis`, y `transform_gold` ahora filtra solo las columnas necesarias y mide memoria en `measure`.

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
| `nivel-2/etl_pipeline-polars/` | **Nuevo:** mismo pipeline modular (extract, silver, gold) con la transformación en Polars. Usa la base `postgres`, no `ny_taxi`. Se corre con `python main.py --stages extract silver gold` |
| `nivel-2/pandas_vs_polars/` | **Nuevo:** benchmarks de la transformación Silver con pandas vs polars (`bench_silver_transform_*.py`, `requirements.txt`) |
| `config/` | Variables de entorno de los servicios |
| `docker-compose.yml` | Airflow, PostgreSQL, MinIO, Redis y demás servicios |

## 4b. Comparación pandas vs Polars (datos reales, enero 2024)

Se ejecutaron ambas versiones sobre los mismos 2.724.160 viajes, 2 corridas cada una:

| Etapa                            | pandas         | Polars          | Mejora |
| Silver (transformar y guardar)  | 6,21 s / 5,66 s | 4,45 s / 4,44 s | ~25 % |
| Gold transformación             | 1,80 s / 1,82 s | 1,24 s / 1,16 s | ~35 % |
| Gold carga (MinIO + PostgreSQL) | 2,09 s / 1,08 s | 1,64 s / 1,48 s | sin diferencia clara |

- Memoria pico en Silver: ~800 MB (Polars) vs ~1.150 MB (pandas). En Gold el pico de Polars fue mayor (~1.335 MB vs ~1.135 MB), pero se midió el proceso completo, así que no es comparable.
- **Resultados distintos:** `gold_route_analysis` tiene 23.528 filas en Polars y 23.259 en pandas; `gold_zone_performance` difiere en una fila. Causa: existe una zona llamada literalmente "N/A"; `pd.read_csv` la convierte en nulo y la pierde, Polars la conserva. Polars es probablemente lo correcto, pero no se corrigió el pipeline pandas.
- Silver en Polars usa enteros más pequeños (Int8 vs Int32) en `pickup_hour` y `pickup_day_of_week`; los valores son iguales.

## 4c. Carga Gold a PostgreSQL: `to_sql` vs `COPY`

Ambas formas hacen lo mismo: dejar las tablas Gold en la base `postgres`. Cambia **cómo viajan las filas**.

- **`to_sql` (lo implementado en `nivel-2/etl_pipeline-polars/load.py`):** Polars → pandas → `to_sql`. Pandas crea la tabla y manda las filas con `INSERT` en lotes. Es como llevar las cajas una por una, pero con un ayudante que se ocupa de etiquetarlas bien.
- **`COPY` (idea alternativa, existe un ejemplo en `nivel-1/etl_pipeline-v1/load.py`):** se crea la tabla con SQLAlchemy, las filas se escriben como un CSV en memoria y se envían de una vez con `COPY ... FROM STDIN WITH CSV`. Es como cargar un camión completo, pero el camión exige que todo esté empacado con un formato exacto.

| | `to_sql` (actual) | `COPY` |
|---|---|---|
| Velocidad | Más lenta; se nota con millones de filas | Mucho más rápida en tablas grandes |
| Con estas tablas (máx. ~23.500 filas) | Casi instantánea, sin diferencia práctica | Igual de rápida; no aporta nada |
| Tipos, nulos y fechas | Los resuelve pandas automáticamente | Hay que cuidarlos a mano: el CSV debe coincidir exactamente con la tabla |
| Código | Corto y simple | Más código y más cosas que pueden fallar |
| Crear la tabla | `to_sql` la crea y la reemplaza sola | Hay que crearla aparte (SQLAlchemy) |
| Re-ejecutar el pipeline | `replace` lo deja igual que antes | Hay que vaciar o recrear la tabla para no duplicar filas |

**Decisión actual:** se usa `to_sql` porque las tablas Gold son pequeñas y la simplicidad importa más que la velocidad. El paso por pandas es solo ese último tramo; toda la transformación sigue en Polars.

**Cuándo cambiar a `COPY`:** si algún día se cargan tablas grandes (por ejemplo, Silver completo, 2,7 millones de filas) a Postgres.

## 4d. Comparación Silver: nivel-1 (pandas) vs nivel-2 (Polars)

*Los tiempos se midieron con `dags/etl_pipeline_v2` (pandas). Su Silver es idéntico al de nivel-1; su Gold es una versión más optimizada.*

| | nivel-1 (pandas) | nivel-2 (Polars) |
|---|---|---|
| Filtro y columnas nuevas | Modifica una copia de la tabla paso a paso | Expresiones que Polars junta y ejecuta en paralelo |
| Nulos | Se chequean a mano (`notna()`) | Comparar contra nulo ya descarta la fila |
| Tiempo (2,7 M filas) | ~5,7–6,2 s | ~4,4 s (≈25 % menos) |
| Memoria pico | ~1.150 MB | ~800 MB |
| Resultado | 2.724.160 filas | 2.724.160 filas (mismas) |
| Tipos | `pickup_hour` y `pickup_day_of_week` en Int32 | Int8 (mismos valores) |

## 4e. Comparación Gold: nivel-1 (pandas) vs nivel-2 (Polars)

| | nivel-1 (pandas) | nivel-2 (Polars) |
|---|---|---|
| Agregaciones | `groupby` | `group_by().agg()`, ordenado por clave |
| Tiempo de transformación | ~1,8 s | ~1,2 s (≈35 % menos) |
| Carga a MinIO/Postgres   | ~1,1–2,1 s | ~1,5–1,6 s (sin diferencia clara; ver 4c) |
| Memoria pico             | ~1.135 MB | ~1.335 MB (no comparable: proceso completo) |
| Zona "N/A"               | La lee como nulo; en `zone_performance` pasa a "Unknown" y en `route_analysis` esos viajes se pierden | La conserva como texto |
| `gold_route_analysis`    | 23.259 filas | 23.528 filas |
| `gold_zone_performance`  | 253 filas | 253 filas (los mismos 9.101 viajes: pandas los llama "Unknown", Polars "N/A") 
| Otras 4 tablas           | Iguales | Iguales |

## 5. Cómo ejecutarlo

1. Levantar los servicios: `docker compose up -d` (para el pipeline bastan Airflow, MinIO, PostgreSQL y Redis).
2. Abrir Airflow en `http://localhost:7777`.
3. Activar el DAG `etl_pipeline` y dispararlo.

Las credenciales (MinIO y PostgreSQL) se leen de variables de entorno; si falta alguna, el pipeline falla con un mensaje claro.

## 6. Pendientes y cosas a tener en cuenta

- **`dags/` ya se versiona** (se quitó del `.gitignore` y se commiteó).
- **Falta probar un fallo real** (por ejemplo, apagar MinIO a mitad de ejecución) para confirmar que el reintento automático (`retries: 1`) funciona. Solo se probó el caso normal y el reintento manual de una tarea.
- **Cada etapa depende de que la anterior haya corrido al menos una vez.** Si Silver no existe en MinIO, Gold falla. Es lo esperado.
- **Solo se procesa enero 2024.** La URL está fija en `config.yml`, aunque el DAG corre `@daily`.
- **Git:** lo anterior ya está commiteado; solo queda sin seguimiento `DE_backup_2026-09-23.docx` (respaldo, se deja fuera a propósito).
- **Contenedores `minio` y `postgres-container` quedaron levantados** tras las pruebas.
- **El DAG viejo `etl_pipeline_v2`** sigue existiendo y convive con el nuevo. Se puede retirar cuando ya no haga falta.
