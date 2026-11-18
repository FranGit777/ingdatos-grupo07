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

1. `extraer_bronze` toma del CSV las URLs con `dateadded` en el intervalo y hace **upsert por `id`** en bronze (`merge`: la URL que ya estaba se actualiza, la nueva se agrega, **ninguna se borra**). Correr dos veces el mismo intervalo deja el mismo resultado, sin duplicados.

   Antes se reemplazaba el intervalo (`overwrite` con `predicate`). Eso supone que el export trae todas las URLs de esas fechas, y no es así: el 18/11/2026 un reproceso de 2018 a hoy dejó a julio de 2026 con 124 URLs en lugar de 4.580. Ver [`INCIDENTE_2026-11-18_backfill.md`](INCIDENTE_2026-11-18_backfill.md).
2. `dbt_build` recalcula silver y gold desde todo bronze.
3. `publicar_gold` reemplaza las tablas de PostgreSQL en una transacción.

`max_active_runs=1` hace que las corridas de un backfill vayan de a una.

## Cómo correrlo

No tenemos la CLI de Airflow en el servidor de la cátedra: todo se hace desde la interfaz, con ▶ → **Trigger DAG w/ config**.

**Un día puntual.** Poner la *Logical date* del día (por ejemplo `2025-03-15`) y dejar `desde` y `hasta` vacíos. La corrida procesa `[data_interval_start, data_interval_end)` de esa fecha. El log de `extraer_bronze` muestra `Intervalo a procesar: [...]` y cuántas URLs había.

**Un rango (meses o años).** Completar los parámetros `desde` (primer día, `AAAA-MM-DD`) y `hasta` (día siguiente al último, excluido). Por ejemplo `desde = 2018-01-01`, `hasta = 2026-11-01` reprocesa todo el histórico hasta octubre de 2026 **en una sola corrida**. El log lista cuántas URLs escribió por mes.

## ¿Qué pasa si hay que reprocesar dos años?

Con el backfill clásico de Airflow serían unas 730 corridas diarias. Cada una descargaría la base completa de URLhaus (la caché de 6 horas ayuda, pero un backfill largo la vence), correría `dbt build` entero y republicaría la gold: horas de trabajo repetido para el mismo resultado, y además sin CLI habría que dispararlas una por una.

Por eso el DAG acepta un **rango en una sola corrida**:

1. Descarga `full.csv.zip` una vez.
2. Lo lee en streaming, agrupa las URLs por mes y hace el mismo upsert por `id` que una corrida diaria. Nunca tiene en memoria más de 300.000 filas: si se llena, vuelca a bronze lo que tiene y sigue.
3. Después corre `dbt build` y `publicar_gold` **una vez**, para todo el rango.

Es idempotente y no pierde datos: correr dos veces el mismo rango deja bronze igual, una corrida diaria encima no duplica, y si el export dejó de traer URLs que ya teníamos, bronze las conserva (probado con un histórico simulado de 7.550 URLs, mezcladas, volcando cada 1.000 filas y con un segundo export al que le faltan 775 URLs).

**Límites:** el export `full.csv.zip` de URLhaus **no es un histórico completo**: trae casi todo el último mes y, de los meses anteriores, solo una parte. Un reproceso no puede recuperar lo que el origen ya no publica; solo completa lo que todavía está. Lo que se acumuló en bronze día a día es la mejor fuente para el pasado, y por eso bronze nunca borra.
