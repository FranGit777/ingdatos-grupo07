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

1. Clonar este repositorio en el JupyterHub del grupo (`/home/jovyan/work`).
2. Abrir `notebooks/NB05_semana10_airflow_dag.ipynb` y correrlo entero. La sección 4 escribe el DAG en `dags/`, copia `dbt/` a `dags/dbt/` y crea `dags/.env` con las variables del JupyterHub; pide la Auth-Key de URLhaus con `getpass`.
3. En https://airflow.nexoacademia.org buscar `pipeline_grupo07`, activarlo y dispararlo con ▶ *Trigger DAG*.
4. Verificar con la sección 5 de NB05, o en Metabase (*Nuevo → Consulta SQL*):

   ```sql
   select familia_malware, sum(cantidad_amenazas) as total
   from grupo07.fct_familias_mensual
   group by 1 order by 2 desc limit 10;
   ```

## Backfill

Cada corrida procesa solo su intervalo de Airflow, nunca la fecha de hoy. Como no tenemos CLI en el servidor, el backfill se dispara desde la interfaz: ▶ → *Trigger DAG w/ config* → poner una **Logical date** pasada (por ejemplo `2025-01-15`) → *Trigger*. Detalle en [`docs/BACKFILL.md`](docs/BACKFILL.md).

## Documentación

- [`docs/GOVERNANCE.md`](docs/GOVERNANCE.md): identificación, origen y linaje, diccionario, sensibilidad, calidad y licencia.
- [`docs/BACKFILL.md`](docs/BACKFILL.md): cómo el DAG reprocesa el pasado sin duplicar.

Fuente de los datos: URLhaus, abuse.ch (https://urlhaus.abuse.ch).
