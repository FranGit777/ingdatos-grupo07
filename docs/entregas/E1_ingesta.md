# Entrega 1 — Ingesta idempotente hacia bronze

**Código:** [`notebooks/NB02_semana04_extraccion_bronze.ipynb`](../../notebooks/NB02_semana04_extraccion_bronze.ipynb)
**Fuente:** [URLhaus](https://urlhaus.abuse.ch) (abuse.ch), base pública de URLs que distribuyen malware.
**Destino:** bronze en Delta Lake, `s3://grupo07/bronze/series` (MinIO).

La ingesta diaria automática la hace la tarea `extraer_bronze` del DAG (E3), con el mismo esquema de bronze. NB02 es la ingesta incremental de E1.

## 1. Cómo correrlo desde cero

En el JupyterHub del grupo:

```bash
cd ~/work
git clone https://github.com/FranGit777/ingdatos-grupo07.git .   # o git pull si ya está clonado
```

1. **Credenciales.** MinIO y PostgreSQL vienen como variables de entorno del JupyterHub. La Auth-Key de URLhaus (gratuita, se pide en https://auth.abuse.ch) va en `dags/.env` como `URLHAUS_AUTH_KEY=...`. Ese archivo no se versiona: lo crea NB05 (sección 4), que pide la clave con `getpass`, o se arma a mano copiando `.env.example`. NB02 la lee de ahí; no hay ninguna clave escrita en el código.
2. Abrir `notebooks/NB02_semana04_extraccion_bronze.ipynb` y correr todas las celdas (*Kernel → Restart & Run All*). La segunda celda instala las dependencias de `requirements.txt`.
3. La sección 5 corre la ingesta **dos veces seguidas** e imprime cuántas filas agregó cada una. La segunda tiene que dar 0. Termina con `✅ Incremental e idempotente`.

Para mirar los datos: la sección 1 imprime el watermark y la cantidad de filas de bronze, y NB03 los consulta con DuckDB.

## 2. El watermark: `id`

El watermark es el **`id` más alto que ya está en bronze**. URLhaus le asigna a cada URL un id entero que crece: una URL reportada hoy siempre tiene un id mayor que cualquiera anterior, así que "traer lo nuevo" es "traer los id mayores al watermark".

- **Se deriva de los datos** (`max(id)` de la tabla Delta), no de un archivo aparte: no puede desincronizarse de lo que realmente hay en bronze.
- **Primera corrida:** si la tabla no existe, el watermark es `None` y se hace la carga histórica desde `full.csv.zip`. Cualquier otro error al leer bronze (MinIO caído, credenciales mal) **no** se toma como tabla vacía: se corta, para no hacer una carga histórica encima de datos que ya existen.
- **Por qué `id` y no la fecha:** varias URLs comparten el mismo segundo de registro, así que con la fecha habría que decidir qué hacer con los empates en el borde; el id es único y crece, no hay empates.

**Límite de la fuente.** Hay tres fuentes con las mismas columnas:

| Fuente | Qué trae | Cuándo se usa |
|---|---|---|
| API `/v1/urls/recent/` | URLs de los últimos 3 días, como máximo 1000 | Corrida normal |
| Export `recent.csv` | Todas las URLs de los últimos 30 días | Cuando la API no alcanza |
| Export `full.csv.zip` | La base completa | Primera corrida, o más de 30 días sin correr |

La API no acepta un parámetro "desde" (ni por fecha ni por id): el watermark no viaja en el pedido sino que filtra la respuesta (`id > watermark`).

El tope de 1000 es poco. En agosto y septiembre de 2026 se reportaban unas 500 URLs por día, y en octubre casi 4.000. Si desde la última corrida entraron 2000 URLs, la API devuelve solo las 1000 más recientes, y sin un control las otras 1000 se perderían en silencio.

El control es este: si la respuesta de la API **no llega hasta el watermark** (ninguna de sus URLs tiene un `id` que ya estaba en bronze), entre lo que tenemos y lo que devolvió puede haber un hueco. Entonces la corrida avisa (`⚠️ La API no llega hasta el watermark`) y trae lo nuevo del export de 30 días, que no tiene tope. Si tampoco ese export llega al watermark, usa la base completa. Así nunca queda un hueco, y en el caso normal el pedido sigue siendo chico.

**Por qué no duplica:** no deduplicamos lo que ya trajimos, directamente no lo escribimos. La escritura es siempre `append` (nunca `overwrite`) y solo con filas cuyo id supera el watermark. La segunda corrida seguida arranca de un watermark ya actualizado y no encuentra nada que escribir.

## 3. Manejo de errores

Un solo punto habla con URLhaus (`pedir()`), y **verifica el código HTTP antes de leer el cuerpo**. Distingue dos tipos de falla:

| Qué pasa | Tipo | Qué hace el código |
|---|---|---|
| La API no responde: timeout (30 s) o error de conexión | transitoria | reintenta: hasta 3 intentos, esperando 5 s y 10 s |
| HTTP 429 (demasiados pedidos) o 5xx (error del servidor) | transitoria | reintenta igual |
| HTTP 401/403 (clave inválida), 404/410 (endpoint retirado), otro 4xx | permanente | corta en el primer intento (`FallaPermanente`): reintentar no lo arregla |
| Responde 200 pero el cuerpo no es JSON, `query_status` no es `ok`/`no_results`, o faltan columnas | permanente | corta sin escribir nada en bronze |
| Sigue fallando después de 3 intentos | transitoria agotada | corta con `FallaTransitoria` y no escribe nada |
| Filas con `id` o fecha ilegibles | dato malo | se descartan esas filas y se informa cuántas |

En ningún caso un error del origen se escribe como si fueran datos: si algo falla, bronze queda como estaba.

**Aviso y registro.** Toda corrida pasa por `correr_ingesta()`. Si corta, por una falla permanente o por una transitoria que agotó los 3 intentos:

- muestra `🚨 INGESTA FALLIDA (falla_permanente)` o `(falla_transitoria)`, con el motivo y qué hacer (volver a correr en un rato, o revisar la clave, el endpoint o el formato);
- anota la falla en un registro de corridas, una tabla Delta aparte en `s3://grupo07/control/ingestas`;
- vuelve a lanzar el error, así la celda queda en rojo y un script u orquestador que la llame se entera.

El registro guarda una fila por corrida, salga bien o mal: inicio y fin, resultado, fuente usada, URLs nuevas, watermark antes y después, y el detalle de la falla (sin la clave). La sección 7 de NB02 lo muestra.

En el DAG (E3) el aviso es el de Airflow: la tarea queda en rojo, con el motivo en el log. Un aviso por correo o chat necesita un servidor de correo configurado en Airflow, que administra la cátedra.

La Auth-Key nunca aparece en los mensajes: va dentro de la URL del export (así lo exige abuse.ch) y se reemplaza por `<AUTH-KEY>`; el texto de las excepciones de `requests`, que incluye la URL, no se muestra.

La sección 6 de NB02 lo demuestra: corre la ingesta contra un endpoint que no existe, recibe 404, corta sin reintentar, avisa y lo registra, y bronze no cambia.

Probado con un URLhaus simulado en 14 casos: primera corrida, incremental, idempotencia, hueco de 2000 URLs con la API llena (se completa con `recent.csv`), hueco de más de 30 días (base completa), API sin resultados, timeout + 503 + éxito, tres 502 seguidos, 401, 410 en el export, cuerpo no JSON, `query_status` desconocido y formato cambiado. En todos bronze queda sin duplicados y cada corrida queda en el registro.
