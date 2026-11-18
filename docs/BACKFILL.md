# Backfill del DAG `pipeline_grupo07`

Cada corrida procesa **solo su intervalo** `[data_interval_start, data_interval_end)`, el que le asigna Airflow. Ninguna tarea usa la fecha de hoy para decidir qué filas procesar, así que un backfill de enero de 2025 trae las URLs de enero de 2025.

## Por qué no usamos `/v1/urls/recent/`

El endpoint `/v1/urls/recent/` de la API de URLhaus no acepta fechas: siempre devuelve lo registrado en los últimos 3 días, con un máximo de 1000 URLs. Con él, un backfill traería las amenazas de hoy. Por eso `extraer_bronze` usa los **volcados CSV** que publica URLhaus, con las mismas columnas que la API:

| Intervalo pedido | Fuente |
| --- | --- |
| Dentro de los últimos 30 días | `https://urlhaus-api.abuse.ch/v2/files/exports/<AUTH-KEY>/recent.csv` |
| Más viejo | `https://urlhaus-api.abuse.ch/v2/files/exports/<AUTH-KEY>/full.csv.zip` (base completa) |

La Auth-Key va dentro de la URL, como lo pide abuse.ch, así que el DAG la reemplaza por `<AUTH-KEY>` en los logs y en los mensajes de error. Las direcciones viejas (`urlhaus.abuse.ch/downloads/...`) responden 410 Gone.

El reloj solo se usa para elegir cuál de los dos volcados descargar, porque el chico alcanza para lo reciente. Las filas siempre se filtran por el intervalo de Airflow. El volcado se guarda en `/tmp` durante 6 horas, así que un backfill de muchos días lo descarga una sola vez.

## Idempotencia

1. `extraer_bronze` toma del CSV las URLs con `dateadded` en el intervalo y **reemplaza en bronze solo las filas de ese intervalo**: `write_deltalake(mode="overwrite", predicate="date_added >= inicio AND date_added < fin")`. Correr dos veces el mismo intervalo deja el mismo resultado, sin duplicados.
2. `dbt_build` recalcula silver y gold desde todo bronze.
3. `publicar_gold` reemplaza las tablas de PostgreSQL en una transacción.

`max_active_runs=1` hace que las corridas de un backfill vayan de a una.

## Cómo correrlo

```bash
airflow dags backfill pipeline_grupo07 -s 2025-01-01 -e 2025-01-31
```

En la UI se ve una corrida por día, con su propio `data_interval_start` y `data_interval_end`. El log de `extraer_bronze` muestra `Intervalo a procesar: [...]` y cuántas URLs había en ese intervalo.
