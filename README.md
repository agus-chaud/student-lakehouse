# Pipeline de datos de taxis de NYC

Pipeline batch que convierte los viajes crudos de taxis amarillos de Nueva York en tablas listas para responder preguntas de negocio: **¿en qué horarios se concentra la demanda?, ¿qué zonas y rutas mueven más viajes e ingresos?, ¿cómo cambian las propinas según la forma de pago?**, etc.

Cada mes de datos llega solo a esas tablas, de forma **automática, verificable y repetible**. Si algo falla en cualquier punto, se reintenta sin perder ni duplicar nada. Hoy tiene cargado **todo 2024: ~35,6 millones de viajes**, procesados mes a mes con **Polars**, guardados en **MinIO** y **PostgreSQL** y orquestados con **Airflow**.

## Hallazgos clave

- **Todo 2024 en ~5 minutos:** 12 ejecuciones mensuales, **36 de 36 tareas en verde**.
- **Polars contra pandas, medido:** en la limpieza, **~25 % más rápido y ~30 % menos memoria** sobre 2,7 M de viajes.
- **Un bug silencioso, encontrado comparando versiones:** pandas convertía en nulo una zona llamada literalmente **"N/A"** y perdía **9.101 viajes** en el análisis de rutas.
- **Idempotente por mes:** reprocesar un mes deja el mismo resultado. Enero dio **749 filas / 2.724.160 viajes** antes y después del rediseño.

## El problema

Un script que descarga, limpia y resume todo de una vez se rompe en cuanto algo sale mal. Si falla el último paso, hay que repetir todo. Si cambia una regla de limpieza, ya no está el dato original. Si se relanza por error, duplica datos. Y un año entero no entra en la memoria de una máquina chica.

---

## Cómo funciona

```
 NYC TLC (internet)        MinIO (archivos)                         PostgreSQL (tablas)
 ┌──────────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────────────────┐
 │ Parquet del  │──►│  BRONZE  │──►│  SILVER  │──►│   GOLD   │──►│ 6 tablas gold_*     │
 │ mes + zonas  │   │  crudo   │   │  limpio  │   │ agregado │   │ con year y month    │
 └──────────────┘   └──────────┘   └──────────┘   └──────────┘   └─────────────────────┘
   Airflow: una ejecución por mes → extract_bronze ──► process_silver ──► process_gold
```

1. **Bronze:** descarga el Parquet del mes y el CSV de 265 zonas, y los guarda **sin tocarlos**. Si ya están, no los vuelve a bajar.
2. **Silver:** descarta los viajes imposibles (sin pasajeros, distancia 0, tarifa negativa, llegada antes de la salida). En enero fueron 240.464 de 2.964.624. Agrega duración, velocidad, hora y % de propina, y procesa **de a un bloque por vez** para no cargar el archivo entero en memoria.
3. **Gold:** calcula 6 tablas de negocio (demanda por hora, zonas, propinas, resumen diario, ingresos por forma de pago y rutas) y las carga a PostgreSQL.

**¿Por qué MinIO para Bronze y Silver?** MinIO es un almacenamiento de objetos compatible con Amazon S3: un "S3 propio" que corre en local. Bronze y Silver son archivos grandes que se escriben una vez y se leen enteros, y para eso es más barato y simple que una base de datos. Además es el **canal entre las tareas de Airflow**: cada una corre en un proceso separado, deja su resultado ahí y la siguiente lo lee. Pasarlo a S3 en la nube es cambiar una dirección en la configuración.

**¿Por qué PostgreSQL para Gold?** Es una base de datos relacional: tablas con tipos, SQL y transacciones. Gold son tablas chicas que consultan analistas y dashboards, así que lo importante es responder rápido con SQL. Las **transacciones** permiten reemplazar un mes "todo o nada", y en eso se apoya la idempotencia.

**¿Qué hace Airflow?** Es el orquestador: decide qué se corre, cuándo y en qué orden, reintenta lo que falla y guarda el historial. No procesa datos.

| Opción | Valor | Para qué |
|---|---|---|
| Tareas | `extract_bronze → process_silver → process_gold` | Si falla Gold, se reintenta solo Gold |
| Horario | `@monthly`, de 2024-01 a 2024-12 | Una ejecución por mes, igual que la fuente |
| Período | `data_interval_start` | La ejecución de marzo siempre procesa marzo, aunque se reintente meses después |
| Concurrencia | `max_active_runs=1` | Un mes por vez, para no pasarse de memoria |
| Reintentos | 1, a los 2 minutos | Recuperarse de fallos transitorios |

Corre con CeleryExecutor (scheduler, worker y Redis) sobre una imagen propia, `airflow-polars:latest`, porque la oficial no trae Polars.

---

## Decisiones técnicas

### 1. Cinco piezas, cada una con un solo trabajo
| Pieza | Responsabilidad |
|---|---|
| `extract.py` | Descargar a Bronze y validar |
| `silver/` | Limpiar y enriquecer |
| `gold.py` + `load.py` | Agregar las 6 tablas y cargarlas a PostgreSQL |
| `utils.py` | Configuración, secretos, período, logs y métricas |
| `main.py` | Elegir qué etapas correr y para qué año y mes |

**Por qué:** cada etapa se corrige y se prueba sola, y `main.py` permite correr etapas sueltas (`--stages gold --year 2024 --month 3`). Eso es lo que hace posible un DAG de 3 tareas reintentables. Los errores **se propagan** en vez de esconderse, así Airflow marca la tarea en rojo y reintenta.

### 2. Idempotencia: correrlo dos veces da lo mismo que una
No vuelve a descargar lo que ya está en Bronze, cada mes escribe solo en su carpeta y en PostgreSQL cada carga hace **`BEGIN → DELETE del mes → INSERT del mes → COMMIT`**. Si el INSERT falla, se deshace todo y el mes anterior queda intacto.
**Por qué:** los pipelines se reintentan todo el tiempo. Antes, Gold reemplazaba la tabla entera: con un solo mes andaba, pero con carga mensual cada ejecución habría borrado los meses anteriores.

### 3. Particionado por fecha
La configuración tiene marcadores `{year}` y `{month}` que se completan en cada ejecución. MinIO usa carpetas `year=2024/month=03/` y las tablas Gold, columnas `year` y `month`.
**Por qué:** reprocesar un mes no toca los demás y la memoria necesaria es la de un mes, no la de un año. No usé el particionamiento nativo de PostgreSQL porque, con este tamaño de tablas, no se justifica la complejidad.

### 4. Polars para transformar, pandas solo para cargar
Migré la transformación a Polars midiendo las dos versiones. La carga sigue con pandas `to_sql`.
**Por qué:** ahí no hubo diferencia medible, las tablas Gold son chicas y prioricé la simplicidad. `COPY` queda como opción si algún día se cargan tablas grandes.

### 5. Backfill explícito en vez de borrar historial
Al pasar el DAG a mensual, Airflow no creó ninguna ejecución de 2024. El catchup arranca desde la **última ejecución registrada**, y la versión diaria anterior ya había corrido después del fin del rango.
**Por qué:** en vez de borrar el historial de Airflow, lancé un backfill explícito de los 12 meses.

### 6. Secretos fuera del repo
El `.env` sale de Git y queda un `.env.example` sin valores. Si falta una credencial, el pipeline frena con un mensaje claro.
**Por qué:** las credenciales no se versionan. Las versiones viejas siguen en el historial; como son de contenedores locales de práctica, no las roté.

---

## Resultados

| Área | Hallazgo | Interpretación |
|---|---|---|
| Volumen 2024 | ~35,6 M de viajes; de 2,60 M (agosto) a 3,30 M (octubre) por mes | El diseño mensual escala a un año en una máquina de 2 núcleos |
| Tiempo por mes | Bronze ~3,6 s · Silver ~7,1 s · Gold + carga ~4,8 s | Silver es el tramo dominante |
| Silver: pandas contra Polars | ~5,9 s → ~4,4 s · ~1.150 MB → ~800 MB | Ejecución en paralelo y menos copias en memoria |
| Calidad (enero) | 240.464 de 2.964.624 viajes descartados (8,1 %) | Se filtran registros físicamente imposibles |
| Consistencia | 23.259 contra 23.528 rutas | Bug de pandas con "N/A"; Bronze intacto permitió probarlo |

## Estructura

```
student-lakehouse/
├── dags/
│   ├── dag_etl_pipeline_polars.py   # DAG mensual de 3 tareas
│   └── etl_pipeline_polars/         # Pipeline: config.yml, extract, silver/, gold, load, utils, main
├── nivel-0/                         # Las mismas capas en SQL (raw/curated, vistas, índices)
├── nivel-1/                         # Primera versión en Python + pandas
├── nivel-2/
│   ├── airflow-image/               # Dockerfile de airflow-polars
│   └── pandas_vs_polars/            # Benchmarks de la comparación
├── docker-compose.yml               # Airflow, PostgreSQL, MinIO, Redis y otros servicios
├── .env.example                     # Plantilla de credenciales
└── ESTADO_PROYECTO.md               # Bitácora técnica con decisiones y mediciones
```

## Cómo correrlo

**Requisitos:** Docker con Compose, unos 8 GB de RAM y 2 núcleos (GitHub Codespaces anda bien).

```bash
cp .env.example .env    # completá las contraseñas
docker build -t airflow-polars:latest -f nivel-2/airflow-image/Dockerfile nivel-2/airflow-image/
docker compose up -d postgres postgres-2 redis minio airflow-init airflow-scheduler airflow-worker airflow-webserver airflow-triggerer
```

Abrí Airflow en el puerto `7777` y activá `etl_pipeline_polars`. Para recuperar todo 2024:

```bash
docker exec student-lakehouse-airflow-scheduler-1 airflow dags backfill -s 2024-01-01 -e 2024-12-01 etl_pipeline_polars
```

> La imagen oficial de MinIO ya no se puede descargar; se usa `bitnamilegacy/minio`, que no trae la consola web.

## Limitaciones y próximos pasos

- **El reintento no se probó con una falla real**, por ejemplo apagando MinIO a mitad de una ejecución.
- **No hay registro propio de ejecuciones** (filas, descartes, duración): hoy queda solo en los logs. Próximo paso: una tabla `pipeline_runs`.
- **El rango termina en diciembre de 2024.** Para datos nuevos haría falta un sensor que espere el archivo, porque la fuente publica con ~2 meses de demora.
- **Sin tests automatizados:** la validación hoy es por conteos y comparación entre versiones.
- **Versiones sin fijar** en la imagen de Airflow, y el `docker-compose.yml` trae servicios que este pipeline no usa (Kafka, Flink, Spark, dbt).
