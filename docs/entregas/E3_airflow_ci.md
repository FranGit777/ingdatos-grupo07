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
| `dbt_build` | Copia `dags/dbt` a `/tmp` y corre `dbt build` (5 modelos + 32 tests) | Si un test falla, la corrida se corta acá y no se publica nada: PostgreSQL conserva la última versión buena |
| `publicar_gold` | Controla que ningún mes haya perdido URLs respecto de la última publicación; si pasa el control, copia las cuatro tablas gold de DuckDB a PostgreSQL en una transacción y verifica que sumen lo mismo | El destino de Metabase tiene sus propias fallas (conexión, permisos); separado, un problema del warehouse no obliga a volver a bajar ni transformar |

Cada tarea hace una cosa, así se ve en el grafo dónde falló y se reintenta solo esa parte. **Nada pesado en el nivel superior del archivo:** al importarlo, el DAG solo define funciones y constantes; las credenciales se leen dentro de cada tarea, y por eso tampoco aparecen en *Rendered Template* de la interfaz.

## 3. Cómo disparar un backfill

Cada corrida procesa su intervalo (`data_interval_start` a `data_interval_end`), que recibe del contexto de Airflow: **ninguna tarea le pregunta la fecha al reloj** para decidir qué datos procesar. Sin CLI en el servidor, se dispara desde la interfaz con ▶ → **Trigger DAG w/ config**:

- **Un día pasado:** poner la *Logical date* y dejar `desde` y `hasta` vacíos. La corrida procesa el día que **termina** en esa fecha, igual que la corrida automática de las 00:00 procesa el día que acaba de cerrar. Ensayado: *Logical date* `2026-11-10` → `Intervalo a procesar: [2026-11-09 03:00 UTC, 2026-11-10 03:00 UTC)`, el 9/11 completo en hora de Córdoba, 914 URLs. Para reprocesar el día X: *Logical date* X + 1, o `desde = X` y `hasta = X + 1`.
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

**Control de volumen:** antes de publicar, `publicar_gold` compara las URLs de cada mes con lo que ya está en PostgreSQL. Como bronze nunca borra, un mes publicado no puede bajar: si baja más de 10 URLs o del 1 %, la tarea falla con los meses afectados en el log (por ejemplo `2026-07: 4600 -> 124`) y no toca las tablas. Es el control que faltaba en el reproceso del 18/11, que pasó todos los tests de ese momento (29; hoy son 32) con julio en 124 URLs. Si la caída es esperada, se vuelve a disparar con ▶ → *Trigger DAG w/ config* y `aceptar_caida = true`. Reintentar no lo arregla: los reintentos de la tarea son para una caída de PostgreSQL.

**Cómo te enterás si frena estando en automático:** cada publicación, exitosa o frenada, deja una fila en `grupo07.estado_publicacion` con el resultado y el motivo. La fila se escribe en una conexión aparte, así que el aviso queda aunque la publicación se deshaga. El tablero tiene un indicador de días sin actualizar que lo lee: en 0 está al día; desde 1, la publicación se frenó o el DAG no corrió.

**Si la corrida falla igual:** la tarea queda en rojo en el grafo, las siguientes no corren y el log de la tarea dice el motivo (por ejemplo `falla permanente (clave, endpoint o pedido), no se reintenta`). PostgreSQL y el tablero conservan la última versión buena. La vista *Grid* guarda el historial de corridas. Un aviso por correo o chat necesita un servidor de correo configurado en Airflow, que administra la cátedra.

**Reintentar es seguro porque todas las tareas son idempotentes:** `extraer_bronze` hace upsert por `id` (correrla dos veces deja bronze igual), dbt recalcula todo desde bronze, y `publicar_gold` reemplaza las tablas dentro de una transacción. Un reintento nunca duplica datos.

## 5. Integración continua

`.github/workflows/ci.yml` corre en **cada push y cada pull request**, con tres jobs en paralelo. Tarda menos de un minuto.

| Job | Qué verifica |
|---|---|
| Sin credenciales en el repo | Que no haya un `.env` versionado ni claves escritas en el código |
| DAG importa en Airflow | Con Airflow 2.10.5: sintaxis de `dags/`, que el DAG importe sin errores, sus 5 tareas y dependencias, que tenga schedule y reintentos, que `extraer_bronze` reciba el intervalo de Airflow y que existan los parámetros del reproceso (`ci/verificar_dag.py`) |
| dbt parse + build con bronze de prueba | Levanta un S3 descartable y corre la tarea real `extraer_bronze` contra tres exports de prueba (`ci/probar_columnas.py`): con las columnas en el orden de URLhaus, en otro orden con una columna nueva (bronze tiene que quedar idéntico) y con una columna faltante (tiene que fallar sin escribir). Después, carga un bronze de prueba con el mismo esquema y la misma ruta que producción (`ci/bronze_de_prueba.py`: un id repetido, Mozi/mozi, IPs, dominios, honeypots, meses sin URLs) y corre `dbt parse` (el SQL compila) y `dbt build` (5 modelos y 32 tests) |

**Bloquea:** `main` está protegida. Todo cambio entra por pull request, los tres checks tienen que estar en verde para poder mergear, y la regla aplica también a los administradores. Un cambio que rompe el DAG o un test no entra.

**Si falla:** el check queda en rojo en el PR con el log del paso que falló, y el botón de merge queda deshabilitado hasta que se arregle con un commit nuevo en la misma rama. Se ve en dos PR que quedan abiertos a propósito: [#25](https://github.com/FranGit777/ingdatos-grupo07/pull/25) rompe el DAG y falla *DAG importa en Airflow*; [#24](https://github.com/FranGit777/ingdatos-grupo07/pull/24) rompe la limpieza de silver y falla *dbt parse + build*.
