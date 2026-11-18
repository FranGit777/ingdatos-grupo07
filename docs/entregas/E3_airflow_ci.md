# Entrega 3 — Orquestación con Airflow e integración continua

**DAG:** [`dags/pipeline_grupo07.py`](../../dags/pipeline_grupo07.py) (lo genera NB05, sección 2).
**CI:** [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml).

```
preparar_entorno ─┬─▶ validar_postgres ─┐
                  └─▶ extraer_bronze ───┴─▶ dbt_build ─▶ publicar_gold
```

## 1. Cómo levantar Airflow y dónde ver el DAG

Airflow lo administra la cátedra en **https://airflow.nexoacademia.org** y lee la carpeta `~/work/dags` del JupyterHub del grupo. No hay que levantar nada; para desplegar:

1. Clonar o actualizar el repo en el JupyterHub (`cd ~/work && git pull`).
2. Correr `notebooks/NB05_semana10_airflow_dag.ipynb` hasta la sección 4. Escribe el DAG en `dags/`, copia el proyecto dbt a `dags/dbt/` (Airflow no ve el resto de `~/work`) y crea `dags/.env` con las credenciales; la Auth-Key de URLhaus se pide con `getpass`.
3. A los ~5 minutos, `pipeline_grupo07` aparece en la lista de DAGs. Se activa con el interruptor y se dispara con ▶.

**Restricciones del servidor que condicionan el diseño:** no podemos crear Variables ni Connections de Airflow (por eso cada tarea lee `dags/.env`) ni instalar paquetes en el worker (por eso `preparar_entorno` arma un entorno propio en `/tmp/venv_grupo07` y las demás tareas corren ahí con `ExternalPythonOperator`).

**Schedule:** `@daily`, a las 00:00 de Córdoba. URLhaus publica todo el día y el export que usamos se regenera continuamente: una corrida por día alcanza y cada una procesa exactamente un día. `start_date` es 2024-01-01 con `catchup=False`: al activar el DAG no se encolan cientos de corridas atrasadas; el reproceso del pasado se pide a propósito (sección 3). `max_active_runs=1` porque dos corridas a la vez compartirían la copia de trabajo de dbt en `/tmp`.

## 2. Qué hace cada tarea y por qué está separada

| Tarea | Qué hace | Por qué es una tarea aparte |
|---|---|---|
| `preparar_entorno` | Crea (una vez) el entorno de Python con deltalake, duckdb, dbt-duckdb y psycopg2, y verifica que exista `dags/.env` | Si falla, es infraestructura (pip, red del worker), no datos. Se reutiliza: no se reinstala en cada corrida |
| `validar_postgres` | Se conecta a `warehouse` y verifica que el usuario pueda crear tablas | Detecta credenciales mal **antes** de bajar y transformar datos; corre en paralelo con la extracción porque no depende de ella |
| `extraer_bronze` | Baja el export de URLhaus del intervalo de la corrida y hace upsert por `id` en bronze | Es la única que habla con el origen: si URLhaus se cae, se reintenta solo esto |
| `dbt_build` | Copia `dags/dbt` a `/tmp` y corre `dbt build` (4 modelos + 29 tests) | Si un test falla, la corrida se corta acá y no se publica nada: PostgreSQL conserva la última versión buena |
| `publicar_gold` | Copia las tres tablas gold de DuckDB a PostgreSQL en una transacción y verifica que sumen lo mismo | El destino de Metabase tiene sus propias fallas (conexión, permisos); separado, un problema del warehouse no obliga a volver a bajar ni transformar |

Cada tarea hace una cosa, así se ve en el grafo dónde falló y se reintenta solo esa parte. **Nada pesado en el nivel superior del archivo:** al importarlo, el DAG solo define funciones y constantes; las credenciales se leen dentro de cada tarea, y por eso tampoco aparecen en *Rendered Template* de la interfaz.

## 3. Cómo disparar un backfill

Cada corrida procesa su intervalo (`data_interval_start` a `data_interval_end`), que recibe del contexto de Airflow: **ninguna tarea le pregunta la fecha al reloj** para decidir qué datos procesar. Sin CLI en el servidor, se dispara desde la interfaz con ▶ → **Trigger DAG w/ config**:

- **Un día pasado:** poner la *Logical date* (por ejemplo `2026-11-10`) y dejar `desde` y `hasta` vacíos. El log de `extraer_bronze` muestra `Intervalo a procesar: [...]` con el día elegido (no el de hoy) y cuántas URLs escribió por mes.
- **Un rango:** completar `desde` y `hasta` (por ejemplo `2026-10-01` y `2026-11-01`). Reprocesa el rango completo **en una sola corrida**.

**¿Qué pasaría con dos años?** Con el backfill clásico serían ~730 corridas diarias, cada una bajando la base completa de URLhaus, corriendo dbt entero y republicando: horas de trabajo repetido, y sin CLI, disparadas una por una. Por eso el DAG acepta el rango en una corrida: baja el export una vez, lo lee en streaming y escribe en bronze mes por mes (nunca más de 300.000 filas en memoria), y después corre dbt y la publicación una sola vez.

**Lo que aprendimos haciéndolo:** el primer reproceso de 2018 a hoy **borró datos**: reemplazaba cada mes de bronze con lo que trae el export, y el export no es un histórico completo. Lo detectamos comparando los conteos por mes, recuperamos las 18.362 URLs desde la silver anterior y cambiamos la escritura a upsert por `id`, que nunca borra. Detalle en [`INCIDENTE_2026-11-18_backfill.md`](../INCIDENTE_2026-11-18_backfill.md) y [`BACKFILL.md`](../BACKFILL.md).

## 4. Reintentos

La pregunta que guía cada número: ¿qué falla esperamos que el reintento resuelva?

| Tarea | Reintentos | Espera | Por qué |
|---|---:|---|---|
| Por defecto (`preparar_entorno`, `publicar_gold`) | 2 | 5 y 10 min (exponencial) | Una caída corta de la red, de MinIO o de PostgreSQL. Más reintentos solo demoran el aviso de un problema real |
| `validar_postgres` | 1 | 2 min | Si las credenciales están mal, reintentar no sirve; el único reintento cubre un reinicio del servidor |
| `extraer_bronze` | 1 (+3 adentro) | 1 y 2 min adentro; 10 min en Airflow | Airflow no distingue un 503 de un 401, así que la tarea decide: timeout, corte de red, 429 o 5xx se reintentan **dentro** de la tarea (3 intentos); 401/403/404/410 cortan en el acto, porque son clave inválida o endpoint retirado. El reintento de Airflow cubre una caída más larga o del worker |
| `dbt_build` | 1 | 2 min | Un test que falla da lo mismo al reintentarlo; el reintento es por si falló la lectura de MinIO |

Además, cada tarea tiene un límite de tiempo (`execution_timeout`: 1 hora, 2 para `extraer_bronze`): una tarea colgada falla en vez de bloquear las corridas siguientes.

**Reintentar es seguro porque todas las tareas son idempotentes:** `extraer_bronze` hace upsert por `id` (correrla dos veces deja bronze igual), dbt recalcula todo desde bronze, y `publicar_gold` reemplaza las tablas dentro de una transacción. Un reintento nunca duplica datos.

## 5. Integración continua

`.github/workflows/ci.yml` corre en **cada push y cada pull request**, con tres jobs en paralelo. Tarda menos de un minuto.

| Job | Qué verifica |
|---|---|
| Sin credenciales en el repo | Que no haya un `.env` versionado ni claves escritas en el código |
| DAG importa en Airflow | Con Airflow 2.10.5: sintaxis de `dags/`, que el DAG importe sin errores, sus 5 tareas y dependencias, que tenga schedule y reintentos, que `extraer_bronze` reciba el intervalo de Airflow y que existan los parámetros del reproceso (`ci/verificar_dag.py`) |
| dbt parse + build con bronze de prueba | Levanta un S3 descartable, carga un bronze de prueba con el mismo esquema y la misma ruta que producción (`ci/bronze_de_prueba.py`: un id repetido, Mozi/mozi, IPs, dominios, honeypots, meses sin URLs) y corre `dbt parse` (el SQL compila) y `dbt build` (los 33 tests) |

**Bloquea:** `main` está protegida. Todo cambio entra por pull request, los tres checks tienen que estar en verde para poder mergear, y la regla aplica también a los administradores. Un cambio que rompe el DAG o un test no entra.

**Si falla:** el check queda en rojo en el PR con el log del paso que falló, y el botón de merge queda deshabilitado hasta que se arregle con un commit nuevo en la misma rama.

## 6. Para la demo (10 minutos)

1. **El DAG en la interfaz:** vista *Graph* (las 5 tareas y sus dependencias) y *Grid* (el historial de corridas).
2. **Una corrida** en verde, o el historial.
3. **Backfill en vivo:** elegir un día en el momento, dispararlo con su *Logical date* y mostrar en el log de `extraer_bronze` que procesó ese día y no el de hoy.
4. **CI en verde y en rojo:** un PR normal con los tres checks en verde, y el PR de demostración que rompe el DAG, con el check en rojo y el merge bloqueado.
