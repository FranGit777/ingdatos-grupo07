# Amenazas URLhaus — grupo07

Proyecto integrador de Ingeniería de Datos (LCD · UCA Rosario · 2026).

Toma el feed público de URLs que distribuyen malware de **URLhaus (abuse.ch)**, lo guarda crudo en el lake (Delta Lake en MinIO), lo limpia y agrega con **dbt + DuckDB**, publica la capa gold en **PostgreSQL** y la consulta en **Metabase**. **Airflow** corre todo una vez por día.

```
URLhaus (export CSV v2)
   │  extraer_bronze: solo las URLs del intervalo [data_interval_start, data_interval_end)
   ▼
BRONZE  s3://grupo07/bronze/series         Delta Lake, tal como lo publica URLhaus
   ▼  dbt
SILVER  stg_urlhaus                        1 fila por URL, tipada, deduplicada, con familia y tipo de archivo
   ▼
GOLD    fct_familias_mensual               mes × tipo × familia × tipo de archivo
        fct_amenazas_mensual               mes × tipo (derivada de la anterior)
        fct_features_familias              features por mes × familia (sin fuga temporal)
   ▼  publicar_gold (una transacción)
PostgreSQL  warehouse.grupo07.*  ──▶  Metabase
```

## Estructura

| Carpeta | Qué hay |
| --- | --- |
| `entorno.py` | Rutas y credenciales a partir de variables de entorno (módulo de la cátedra) |
| `notebooks/` | NB01–NB06, uno por etapa. **NB05 genera `dags/`, `dbt/` y `docs/`** |
| `dags/` | DAG `pipeline_grupo07` |
| `dbt/` | Modelos silver/gold y tests |
| `docs/` | `GOVERNANCE.md` y `BACKFILL.md` |
| `.env.example` | Variables que necesita `dags/.env` (el `.env` real no se versiona) |

> Los archivos de `dags/`, `dbt/` y `docs/` salen de las celdas de NB05. Para cambiar algo se cambia la celda, se corre NB05 y se commitean los archivos generados.

## Cómo correrlo (servidor de la cátedra)

1. Clonar este repositorio en el JupyterHub del grupo (`/home/jovyan/work`). Las dependencias de los notebooks están en `requirements.txt` y cada notebook las instala en su segunda celda.
2. Abrir `notebooks/NB05_semana10_airflow_dag.ipynb` y correrlo entero. La sección 4 escribe el DAG en `dags/`, copia `dbt/` a `dags/dbt/` y crea `dags/.env` con las variables del JupyterHub; pide la Auth-Key de URLhaus con `getpass`.
3. En https://airflow.nexoacademia.org buscar `pipeline_grupo07`, activarlo y dispararlo con ▶ *Trigger DAG*.
4. Verificar con la sección 5 de NB05, o en Metabase (*Nuevo → Consulta SQL*):

   ```sql
   select familia_malware, sum(cantidad_amenazas) as total
   from grupo07.fct_familias_mensual
   group by 1 order by 2 desc limit 10;
   ```

## Backfill

Cada corrida procesa solo su intervalo, nunca la fecha de hoy. Sin CLI en el servidor, se dispara desde la interfaz con ▶ → *Trigger DAG w/ config*:

- **Un día:** una *Logical date* pasada (por ejemplo `2025-03-15`), con `desde` y `hasta` vacíos.
- **Un rango:** los parámetros `desde` y `hasta` (por ejemplo `2018-01-01` y `2026-11-01`). Reprocesa todo el rango en una sola corrida, mes por mes, sin duplicar.

Detalle y límites en [`docs/BACKFILL.md`](docs/BACKFILL.md).

## Entregas

| Entrega | Qué es | Documento |
| --- | --- | --- |
| E1 | Ingesta incremental e idempotente hacia bronze | [`docs/entregas/E1_ingesta.md`](docs/entregas/E1_ingesta.md) |
| E2 | Transformación con dbt y calidad de datos | [`docs/entregas/E2_dbt.md`](docs/entregas/E2_dbt.md) |
| E3 | Orquestación con Airflow e integración continua | [`docs/entregas/E3_airflow_ci.md`](docs/entregas/E3_airflow_ci.md) |
| E4 | Serving, features y governance | [`docs/entregas/E4_serving_governance.md`](docs/entregas/E4_serving_governance.md) |

## Documentación

- [`docs/GOVERNANCE.md`](docs/GOVERNANCE.md): identificación, origen y linaje, diccionario, sensibilidad, calidad y licencia.
- [`docs/BACKFILL.md`](docs/BACKFILL.md): cómo el DAG reprocesa el pasado sin duplicar.
- [`docs/TABLERO.md`](docs/TABLERO.md): el tablero de Metabase, las preguntas que responde y su captura.
- [`docs/INCIDENTE_2026-11-18_backfill.md`](docs/INCIDENTE_2026-11-18_backfill.md): el reproceso que borró datos, cómo se recuperaron y qué cambiamos.

Fuente de los datos: URLhaus, abuse.ch (https://urlhaus.abuse.ch). Uso académico y no comercial, según los [términos de abuse.ch](https://abuse.ch/terms-of-use/).

## Integración continua

`.github/workflows/ci.yml` corre en cada push y en cada pull request, con tres jobs en paralelo:

| Job | Qué verifica |
| --- | --- |
| Sin credenciales en el repo | Que no haya un `.env` versionado ni claves escritas en el código |
| DAG importa en Airflow | Con Airflow 2.10.5: que `dags/` importe sin errores y que `pipeline_grupo07` tenga sus 5 tareas, las dependencias, el schedule, los reintentos y el intervalo de Airflow en `extraer_bronze` (`ci/verificar_dag.py`) |
| dbt parse + build | Levanta un S3 descartable (moto), carga un bronze de prueba con la misma ruta y esquema que el DAG (`ci/bronze_de_prueba.py`) y corre `dbt parse` y `dbt build` con todos los tests |

Si un job falla, el check queda en rojo y la protección de rama de `main` impide mergear el cambio.

## Cómo trabajar con el repo

- `main` está protegida: todo cambio entra por *pull request* y necesita el CI en verde.
- En el JupyterHub, cerrar los notebooks antes de `git pull`. Si Jupyter pregunta *Overwrite / Revert*, elegir **Revert** (si no, pisa la versión nueva con la vieja).
- `dags/`, `dbt/` y `docs/GOVERNANCE.md` los genera NB05: se editan sus celdas, no los archivos. Después de un `git pull`, correr NB05 para que Airflow tome los cambios.
- Al reiniciar el Hub se pierde lo instalado con `pip`: correr la celda de instalación de cada notebook.
- Las credenciales van en `dags/.env`, que nunca se commitea (ver `.env.example`).
- Para regenerar el diagrama de linaje: `python scripts/linaje.py` (necesita graphviz).
