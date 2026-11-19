"""
DAG pipeline_grupo07 — URLhaus -> bronze (Delta en MinIO) -> dbt (silver/gold en DuckDB) -> PostgreSQL.

Gold publicada: fct_familias_mensual (atómica: por mes, tipo de amenaza, familia de malware
y tipo de archivo, sacados de los tags de URLhaus), fct_amenazas_mensual (su resumen por tipo)
y fct_features_familias (features por mes y familia, sin fuga temporal).

Ingeniería de Datos — LCD · UCA Rosario · 2026 · grupo07

Cómo corre en el servidor de la cátedra (Airflow 2.x):
  * El worker de Airflow NO trae deltalake, duckdb ni dbt. La tarea `preparar_entorno`
    crea UNA vez un virtualenv en /tmp/venv_grupo07 con todo lo necesario, y las demás
    tareas corren dentro de ese venv con ExternalPythonOperator (no se reinstala nada
    en cada corrida).
  * Airflow solo ve esta carpeta (dags/ del JupyterHub = /opt/airflow/dags/grupo07/).
    Por eso las credenciales se leen de `dags/.env` y dbt se toma de `dags/dbt/`.
  * Las credenciales se leen DENTRO de cada tarea: no se pasan por op_kwargs ni env=,
    así no quedan visibles en la pestaña "Rendered Template" de la UI de Airflow.

Backfill: cada corrida procesa SOLO su intervalo [data_interval_start, data_interval_end).
  extraer_bronze baja el volcado CSV de URLhaus (últimos 30 días o base completa), filtra
  las URLs registradas en ese intervalo y hace upsert por id en bronze (no borra). Correr dos
  veces el mismo intervalo da el mismo resultado. dbt y publicar_gold recalculan todo
  desde bronze, así que también son idempotentes.

Tareas:
  preparar_entorno -> [validar_postgres, extraer_bronze] -> dbt_build -> publicar_gold
"""
from __future__ import annotations

import os

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import ExternalPythonOperator

AQUI = os.path.dirname(os.path.realpath(__file__))
ENV_FILE = os.path.join(AQUI, ".env")
DBT_SRC = os.path.join(AQUI, "dbt")
DBT_RUN = "/tmp/dbt_run_grupo07"          # copia de trabajo de dbt (dags/ puede ser solo lectura)
VENV = "/tmp/venv_grupo07"
VENV_PY = f"{VENV}/bin/python"
# Si cambiás esta lista, subí VENV_VERSION para que el venv se reconstruya.
VENV_REQS = "dbt-duckdb deltalake pandas pyarrow requests psycopg2-binary"
VENV_VERSION = "v1"


# ----------------------------------------------------------------------------------
# Las funciones de abajo corren dentro del venv (ExternalPythonOperator): Airflow
# copia SOLO el código de la función, así que cada una importa y define lo que usa.
# ----------------------------------------------------------------------------------
def _validar_postgres(env_file):
    """Valida credenciales de PostgreSQL y que haya un esquema donde podamos crear tablas."""
    import os
    import psycopg2

    for linea in open(env_file, encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    con = psycopg2.connect(host=os.environ.get("PGHOST", "postgres"),
                           port=os.environ.get("PGPORT", "5432"),
                           dbname=os.environ.get("PGDATABASE", "warehouse"),
                           user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
                           connect_timeout=10)
    with con, con.cursor() as cur:
        cur.execute("select current_user, current_database(), version()")
        usuario, base, version = cur.fetchone()
        print(f"Conectado a '{base}' como '{usuario}' ({version.split(',')[0]})")
        cur.execute("""
            select nspname from pg_namespace
            where has_schema_privilege(nspname, 'CREATE')
              and nspname not like 'pg\\_%' and nspname <> 'information_schema'
            order by (nspname = current_user) desc, (nspname = 'public') desc, nspname
        """)
        esquemas = [r[0] for r in cur.fetchall()]
    con.close()
    pedido = os.environ.get("PG_SCHEMA")
    if pedido and pedido not in esquemas:
        raise RuntimeError(f"PG_SCHEMA={pedido} pero el usuario no puede crear tablas ahí. "
                           f"Esquemas con permiso CREATE: {esquemas}")
    if not esquemas:
        raise RuntimeError("El usuario no tiene permiso CREATE en ningún esquema: pedile a la "
                           "cátedra un esquema propio (p. ej. 'grupo07').")
    print(f"Esquemas donde se pueden crear tablas: {esquemas}. Se usará: {pedido or esquemas[0]}")


def _extraer_bronze(env_file, inicio, fin):
    """Ingesta URLhaus -> bronze (Delta) del intervalo [inicio, fin) que le pasa Airflow.

    Idempotente y apta para backfill: el resultado depende solo del intervalo, no de la
    fecha en que corre. Hace upsert por id: agrega las URLs nuevas, actualiza las que ya
    estaban y no borra ninguna.
    """
    import csv
    import io
    import os
    import time
    import zipfile
    import pandas as pd
    import pyarrow as pa
    import requests
    from deltalake import DeltaTable, write_deltalake
    from deltalake.exceptions import TableNotFoundError

    for linea in open(env_file, encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    bronze = f"s3://{os.environ.get('MINIO_BUCKET', 'grupo07')}/bronze/series"
    opciones = {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_ENDPOINT_URL": os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    clave = os.environ.get("URLHAUS_AUTH_KEY")
    if not clave:
        raise RuntimeError("Falta URLHAUS_AUTH_KEY en dags/.env")
    cabecera = {"Auth-Key": clave}

    # 1) El intervalo viene del contexto de la corrida, nunca del reloj: el data_interval que
    #    asigna Airflow o, en un reproceso, los parámetros desde/hasta del Trigger.
    def utc(texto):
        t = pd.Timestamp(texto)
        return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
    desde, hasta = utc(inicio), utc(fin)
    if desde >= hasta:
        raise ValueError(f"Intervalo vacío o al revés: [{desde}, {hasta})")
    print(f"Intervalo a procesar: [{desde}, {hasta})")

    # 2) Fuente. La API /urls/recent/ solo devuelve los últimos 3 días (máx. 1000 URLs), así que
    #    no sirve para un backfill. URLhaus publica dos volcados CSV con todas las columnas:
    #    los últimos 30 días y la base completa. Se usa el más chico que cubre el intervalo.
    #    El reloj solo elige la fuente; el filtro de filas es siempre por el intervalo.
    #    La Auth-Key va dentro de la URL (así lo exige abuse.ch): nunca se imprime.
    ahora = pd.Timestamp.now(tz="UTC")
    base_exports = "https://urlhaus-api.abuse.ch/v2/files/exports/{clave}"
    url_reciente = os.environ.get("URLHAUS_CSV_RECIENTE", base_exports + "/recent.csv").format(clave=clave)
    url_completo = os.environ.get("URLHAUS_CSV_COMPLETO", base_exports + "/full.csv.zip").format(clave=clave)
    if desde >= ahora - pd.Timedelta(days=29):
        url_csv, archivo = url_reciente, "/tmp/urlhaus_grupo07/csv_recent"
    else:
        url_csv, archivo = url_completo, "/tmp/urlhaus_grupo07/csv_completo"

    # El volcado se guarda en /tmp y se reutiliza durante 6 horas: un backfill de muchos días
    # lo descarga una sola vez.
    os.makedirs(os.path.dirname(archivo), exist_ok=True)
    if not os.path.exists(archivo) or time.time() - os.path.getmtime(archivo) > 6 * 3600:
        visible = url_csv.replace(clave, "<AUTH-KEY>")
        # Fallas transitorias (timeout, corte de red, HTTP 429 o 5xx): se reintenta acá adentro,
        # 3 intentos esperando 1 y 2 minutos. Fallas permanentes (401/403 clave inválida, 404/410
        # endpoint retirado, otro 4xx): se corta en el acto; reintentar no lo arregla. Airflow
        # no distingue una de otra, por eso la decisión se toma en la tarea.
        # Los errores de requests incluyen la URL (con la clave): nunca se muestran.
        intentos = 3
        for intento in range(1, intentos + 1):
            print(f"Descargando {visible} (intento {intento}/{intentos})")
            try:
                with requests.get(url_csv, headers=cabecera, timeout=600, stream=True) as res:
                    if res.status_code == 200:
                        with open(archivo + ".parcial", "wb") as f:
                            for bloque in res.iter_content(1 << 20):
                                f.write(bloque)
                        break
                    if res.status_code != 429 and res.status_code < 500:
                        raise RuntimeError(f"URLhaus respondió {res.status_code} al bajar {visible}: "
                                           "falla permanente (clave, endpoint o pedido), no se reintenta")
                    motivo = f"HTTP {res.status_code}"
            except requests.RequestException as e:
                motivo = type(e).__name__
            if intento == intentos:
                raise RuntimeError(f"URLhaus no respondió bien en {intentos} intentos ({motivo}) al bajar {visible}")
            print(f"  falló ({motivo}); reintento en {intento} min")
            time.sleep(60 * intento)
        os.replace(archivo + ".parcial", archivo)

    # 3) Leer el CSV en streaming (la base completa es grande) y escribir en bronze por tandas
    #    mensuales. Formato de URLhaus: líneas de comentario con '#', fechas
    #    'YYYY-MM-DD HH:MM:SS' en UTC. Nunca se tienen en memoria más de 300.000 filas.
    columnas = ['id', 'date_added', 'url', 'url_status', 'last_online',
                'threat', 'tags', 'urlhaus_link', 'reporter']
    esquema = pa.schema([
        ("id", pa.int64()), ("date_added", pa.timestamp("us", tz="UTC")),
        ("url", pa.string()), ("url_status", pa.string()), ("last_online", pa.string()),
        ("threat", pa.string()), ("tags", pa.string()),
        ("urlhaus_link", pa.string()), ("reporter", pa.string()),
    ])
    texto_desde = desde.strftime("%Y-%m-%d %H:%M:%S")
    texto_hasta = hasta.strftime("%Y-%m-%d %H:%M:%S")

    def a_tabla(filas):
        df = pd.DataFrame(filas, columns=columnas)
        df["id"] = df["id"].astype("int64")
        df["date_added"] = pd.to_datetime(df["date_added"], utc=True)
        # tags como los publica URLhaus ("Mozi,elf"), sin espacios de más; silver los separa
        df["tags"] = df["tags"].map(lambda t: ",".join(x.strip() for x in t.split(",") if x.strip()) or None)
        for col in ["url", "url_status", "last_online", "threat", "urlhaus_link", "reporter"]:
            df[col] = df[col].where(df[col].str.strip().ne("") & df[col].ne("None"), None)
        df = df.drop_duplicates(subset=["id"])
        return pa.Table.from_pandas(df, schema=esquema, preserve_index=False)

    try:
        DeltaTable(bronze, storage_options=opciones)
        existe = [True]
    except TableNotFoundError:
        existe = [False]
    escritos = {}           # mes -> URLs escritas en esta corrida

    def volcar(clave_mes, filas):
        # 4) Upsert por id: la URL que ya está en bronze se actualiza (por ejemplo, su estado
        #    online/offline), la nueva se agrega y NINGUNA se borra. El export de URLhaus no es un
        #    histórico completo (de los meses viejos solo trae parte de las URLs), así que
        #    reemplazar el intervalo borraría datos que ya teníamos: pasó el 18/11/2026, ver
        #    docs/INCIDENTE_2026-11-18_backfill.md. Correr dos veces lo mismo deja lo mismo.
        tabla = a_tabla(filas)
        if existe[0]:
            # Se adapta al esquema real de bronze (columna por columna) antes del merge, que
            # exige tipos iguales en origen y destino.
            destino = DeltaTable(bronze, storage_options=opciones).to_pyarrow_dataset().schema
            tabla = tabla.select(destino.names).cast(destino)
        if not existe[0]:
            write_deltalake(bronze, tabla, mode="append", storage_options=opciones)
            existe[0] = True
        else:
            (DeltaTable(bronze, storage_options=opciones)
             .merge(source=tabla, predicate="t.id = s.id", source_alias="s", target_alias="t",
                    merge_schema=True)
             .when_matched_update_all()
             .when_not_matched_insert_all()
             .execute())
        escritos[clave_mes] = escritos.get(clave_mes, 0) + tabla.num_rows

    if zipfile.is_zipfile(archivo):
        zf = zipfile.ZipFile(archivo)
        crudo = zf.open(zf.namelist()[0])
    else:
        crudo = open(archivo, "rb")
    MAX_EN_MEMORIA = 300_000
    pendientes, en_memoria = {}, 0
    with io.TextIOWrapper(crudo, encoding="utf-8", errors="replace", newline="") as lineas:
        sin_comentarios = (l for l in lineas if not l.startswith("#"))
        for r in csv.reader(sin_comentarios):
            if len(r) < len(columnas) or not r[0].strip().isdigit():
                continue
            if texto_desde <= r[1] < texto_hasta:
                pendientes.setdefault(r[1][:7], []).append(r[:len(columnas)])
                en_memoria += 1
                if en_memoria >= MAX_EN_MEMORIA:
                    for clave_mes in sorted(pendientes):
                        volcar(clave_mes, pendientes[clave_mes])
                    pendientes, en_memoria = {}, 0
    for clave_mes in sorted(pendientes):
        volcar(clave_mes, pendientes[clave_mes])

    total = sum(escritos.values())
    if not total:
        print("Nada para escribir en este intervalo (bronze queda como estaba).")
        return 0
    for clave_mes in sorted(escritos):
        print(f"  {clave_mes}: {escritos[clave_mes]} URLs")
    # stg_urlhaus lee los .parquet directamente: se borran los archivos que el merge dejó
    # fuera de la tabla, para que dbt no lea versiones viejas.
    DeltaTable(bronze, storage_options=opciones).vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=False)
    print(f"Bronze: {total} URLs del intervalo (upsert por id) en {len(escritos)} mes(es).")
    return total


def _dbt_build(env_file, dbt_src, dbt_run, venv):
    """Copia el proyecto dbt a /tmp y corre `dbt build` (modelos + tests)."""
    import os
    import shutil
    import subprocess

    for linea in open(env_file, encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    shutil.rmtree(dbt_run, ignore_errors=True)
    shutil.copytree(dbt_src, dbt_run, ignore=shutil.ignore_patterns(
        "target", "logs", ".ipynb_checkpoints", "*.duckdb", "*.duckdb.wal"))

    env = dict(os.environ)
    env["HOME"] = "/tmp/home_grupo07"      # DuckDB guarda sus extensiones en $HOME
    os.makedirs(env["HOME"], exist_ok=True)
    env["PATH"] = f"{venv}/bin:" + env.get("PATH", "")
    r = subprocess.run([f"{venv}/bin/dbt", "build", "--profiles-dir", "."],
                       cwd=dbt_run, env=env, capture_output=True, text=True)
    print(r.stdout)
    print(r.stderr)
    if r.returncode != 0:
        raise RuntimeError(f"dbt build terminó con código {r.returncode}")


def _publicar_gold(env_file, dbt_run, aceptar_caida="False"):
    """Publica las tablas gold de DuckDB en PostgreSQL (lo que lee Metabase).

    Antes de reemplazar nada, compara las URLs de cada mes con la última publicación: si un
    mes perdió URLs, corta (control de volumen). aceptar_caida="True" publica igual."""
    import os
    import duckdb
    import psycopg2
    from psycopg2.extras import execute_values

    for linea in open(env_file, encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    # Cada tabla gold: columnas a leer de DuckDB (con su tipo) y la clave primaria.
    # El orden de las columnas es el mismo en el select, en el create y en el insert.
    TABLAS = {
        "fct_amenazas_mensual": {
            "columnas": [("anio", "integer"), ("mes", "integer"), ("tipo_amenaza", "text"),
                         ("cantidad_amenazas", "bigint")],
            "clave": ["anio", "mes", "tipo_amenaza"],
        },
        "fct_familias_mensual": {
            "columnas": [("fecha", "date"), ("anio", "integer"), ("mes", "integer"),
                         ("tipo_amenaza", "text"), ("familia_malware", "text"),
                         ("tipo_archivo", "text"), ("cantidad_amenazas", "bigint")],
            "clave": ["anio", "mes", "tipo_amenaza", "familia_malware", "tipo_archivo"],
        },
        # Features (E4): definidos en dbt (fct_features_familias); acá solo se copian
        "fct_features_familias": {
            "columnas": [("fecha", "date"), ("anio", "integer"), ("mes", "integer"),
                         ("familia_malware", "text"), ("cantidad_amenazas", "bigint"),
                         ("cantidad_mes_anterior", "bigint"), ("variacion_mensual_pct", "numeric(14,2)"),
                         ("promedio_movil_3m", "numeric(14,2)"), ("acumulado", "bigint"),
                         ("pct_del_mes", "numeric(5,2)"), ("mes_completo", "boolean")],
            "clave": ["fecha", "familia_malware"],
            "nulos": {"variacion_mensual_pct"},     # vacía si el mes anterior tuvo 0 URLs
        },
    }

    # 1) Leer gold de DuckDB (lo dejó dbt_build)
    ruta = os.path.join(dbt_run, "warehouse.duckdb")
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No existe {ruta}: ¿corrió dbt_build en este mismo worker?")
    datos = {}
    con = duckdb.connect(ruta, read_only=True)
    try:
        for nombre, t in TABLAS.items():
            select = ", ".join(f"cast({c} as {'varchar' if tipo == 'text' else tipo})"
                               for c, tipo in t["columnas"])
            datos[nombre] = con.sql(f"select {select} from {nombre}").fetchall()
            if not datos[nombre]:
                raise RuntimeError(f"{nombre} está vacía en DuckDB: no publico una tabla vacía.")
    finally:
        con.close()

    # Descripciones de schema.yml (dbt las deja en target/manifest.json): se copian como COMMENT
    # en PostgreSQL, así la documentación se escribe una sola vez y Metabase la muestra.
    import json
    docs = {}
    ruta_manifest = os.path.join(dbt_run, "target", "manifest.json")
    if os.path.exists(ruta_manifest):
        for nodo in json.load(open(ruta_manifest, encoding="utf-8"))["nodes"].values():
            if nodo.get("resource_type") == "model" and nodo["name"] in TABLAS:
                docs[nodo["name"]] = nodo

    # 2) Escribir en PostgreSQL en UNA transacción: si algo falla, quedan las versiones anteriores
    pg = psycopg2.connect(host=os.environ.get("PGHOST", "postgres"),
                          port=os.environ.get("PGPORT", "5432"),
                          dbname=os.environ.get("PGDATABASE", "warehouse"),
                          user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
                          connect_timeout=10)
    resultado = {}
    try:
        with pg, pg.cursor() as cur:
            esquema = os.environ.get("PG_SCHEMA")
            if not esquema:
                cur.execute("""
                    select nspname from pg_namespace
                    where has_schema_privilege(nspname, 'CREATE')
                      and nspname not like 'pg\\_%' and nspname <> 'information_schema'
                    order by (nspname = current_user) desc, (nspname = 'public') desc, nspname
                    limit 1
                """)
                fila = cur.fetchone()
                if not fila:
                    raise RuntimeError("Sin permiso CREATE en ningún esquema de PostgreSQL.")
                esquema = fila[0]

            # Control de volumen. Bronze nunca borra, así que un mes ya publicado no puede
            # tener menos URLs que antes. Si las tiene, algo se perdió en el camino (como en el
            # reproceso del 18/11/2026, que dejó julio en 124 URLs en lugar de 4.580, con todos
            # los tests en verde). Se corta acá, antes de tocar las tablas: el tablero sigue
            # mostrando la última versión buena. Margen: hasta 10 URLs o el 1 % del mes.
            cur.execute("select to_regclass(%s)", (f'"{esquema}".fct_amenazas_mensual',))
            if cur.fetchone()[0] is not None:
                cur.execute(f'select anio, mes, sum(cantidad_amenazas) '
                            f'from "{esquema}".fct_amenazas_mensual group by 1, 2')
                publicados = {(int(a), int(m)): int(n) for a, m, n in cur.fetchall()}
                nuevos = {}
                for anio, mes, _, n in datos["fct_amenazas_mensual"]:
                    nuevos[(anio, mes)] = nuevos.get((anio, mes), 0) + int(n)
                caidas = [(a, m, antes, nuevos.get((a, m), 0))
                          for (a, m), antes in sorted(publicados.items())
                          if antes - nuevos.get((a, m), 0) > max(10, antes * 0.01)]
                if not caidas:
                    print(f"Control de volumen OK: ningún mes bajó más de 10 URLs o del 1 % "
                          f"({len(publicados)} meses comparados con la última publicación).")
                else:
                    detalle = "; ".join(f"{a}-{m:02d}: {antes} -> {ahora}" for a, m, antes, ahora in caidas)
                    if str(aceptar_caida).strip().lower() != "true":
                        raise RuntimeError(
                            f"Control de volumen: {len(caidas)} mes(es) tienen menos URLs que en la última "
                            f"publicación ({detalle}). No se publica nada. Revisar bronze; si la caída es "
                            "esperada, volver a disparar el DAG con aceptar_caida = true.")
                    print(f"⚠️  Control de volumen: bajan {len(caidas)} mes(es) ({detalle}). "
                          "Se publica igual porque se pidió aceptar_caida.")

            for nombre, t in TABLAS.items():
                tabla = f'"{esquema}".{nombre}'
                columnas = ",\n".join(f"{c} {tipo}{'' if c in t.get('nulos', ()) else ' not null'}"
                                       for c, tipo in t["columnas"])
                chequeo = ", check (mes between 1 and 12)" if any(c == "mes" for c, _ in t["columnas"]) else ""
                # Si la tabla ya existe con otras columnas (cambió el modelo), se recrea.
                # Sin CASCADE: si una vista depende de ella, falla y avisa en vez de borrarla.
                cur.execute("""
                    select column_name from information_schema.columns
                    where table_schema = %s and table_name = %s
                """, (esquema, nombre))
                actuales = {r[0] for r in cur.fetchall()}
                esperadas = {c for c, _ in t["columnas"]} | {"actualizado_en"}
                if actuales and actuales != esperadas:
                    print(f"{tabla}: cambiaron las columnas ({sorted(actuales ^ esperadas)}), la recreo.")
                    cur.execute(f"drop table {tabla}")
                cur.execute(f"""
                    create table if not exists {tabla} (
                        {columnas},
                        actualizado_en timestamptz not null default now(),
                        primary key ({", ".join(t["clave"])}){chequeo}
                    )
                """)
                d = docs.get(nombre, {})
                if d.get("description"):
                    cur.execute(f"comment on table {tabla} is %s", (" ".join(d["description"].split()),))
                for col, info in d.get("columns", {}).items():
                    if info.get("description") and col in {c for c, _ in t["columnas"]}:
                        cur.execute(f'comment on column {tabla}."{col}" is %s', (info["description"],))
                cur.execute(f"comment on column {tabla}.actualizado_en is %s",
                            ("Momento en que el DAG pipeline_grupo07 publicó la fila.",))
                cur.execute(f"truncate {tabla}")
                lista = ", ".join(c for c, _ in t["columnas"])
                execute_values(cur, f"insert into {tabla} ({lista}) values %s", datos[nombre])
                cur.execute(f"select count(*), sum(cantidad_amenazas) from {tabla}")
                resultado[tabla] = cur.fetchone()
    finally:
        pg.close()
    for (tabla, (n, total)), nombre in zip(resultado.items(), TABLAS):
        if n != len(datos[nombre]):
            raise RuntimeError(f"Se esperaban {len(datos[nombre])} filas en {tabla} y hay {n}.")
        print(f"Publicadas {n} filas ({total} amenazas) en PostgreSQL: {tabla}")
    totales = {total for n, total in resultado.values()}
    if len(totales) != 1:
        raise RuntimeError(f"Las tablas gold no suman lo mismo: {resultado}")
    return sum(n for n, _ in resultado.values())


# ---------------------------------------------------------------------------- DAG
# Reintentos: para lo que puede resolverse solo (el worker se reinició, PostgreSQL o MinIO no
# respondieron un rato). Todas las tareas son idempotentes, así que reintentar nunca duplica.
# 2 reintentos con espera creciente (5 y 10 min) cubren una caída corta sin esconder un problema
# real; más que eso solo demora el aviso. Cada tarea ajusta según qué falla puede tener.
default_args = {
    "owner": "grupo07",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    "execution_timeout": pendulum.duration(hours=1),   # una tarea colgada no bloquea el DAG
}

with DAG(
    dag_id="pipeline_grupo07",
    description="URLhaus -> bronze (Delta) -> dbt silver/gold -> PostgreSQL",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Cordoba"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,          # dos corridas a la vez pisarían /tmp/dbt_run_grupo07
    default_args=default_args,
    tags=["proyecto-integrador", "grupo07"],
    # Reproceso de un rango en UNA corrida (Trigger DAG w/ config). Vacíos = el intervalo normal
    # de la corrida. Ver docs/BACKFILL.md.
    params={
        "desde": Param(None, type=["null", "string"],
                       description="Reproceso: primer día a reprocesar (AAAA-MM-DD, UTC). Vacío = intervalo de la corrida."),
        "hasta": Param(None, type=["null", "string"],
                       description="Reproceso: día siguiente al último a reprocesar (AAAA-MM-DD, UTC, excluido)."),
        "aceptar_caida": Param(False, type="boolean",
                               description="Publicar aunque algún mes tenga menos URLs que en la última publicación."),
    },
) as dag:

    preparar_entorno = BashOperator(
        task_id="preparar_entorno",
        bash_command=f"""
        set -e
        if [ ! -f {VENV}/.listo_{VENV_VERSION} ]; then
            rm -rf {VENV}
            python3 -m venv {VENV}
            {VENV}/bin/pip install --quiet --upgrade pip
            {VENV}/bin/pip install --quiet {VENV_REQS}
            touch {VENV}/.listo_{VENV_VERSION}
        fi
        {VENV_PY} -c "import deltalake, duckdb, pandas, psycopg2, requests; print('venv OK')"
        test -f {ENV_FILE} || (echo "Falta {ENV_FILE}: corré la celda de despliegue de NB05" && exit 1)
        """,
    )

    comunes = dict(python=VENV_PY, expect_airflow=False)

    # Credenciales mal no se arreglan reintentando: un solo reintento, por si el servidor se
    # estaba reiniciando.
    validar_postgres = ExternalPythonOperator(
        task_id="validar_postgres", python_callable=_validar_postgres,
        op_kwargs={"env_file": ENV_FILE}, retries=1,
        retry_delay=pendulum.duration(minutes=2), **comunes)

    # El intervalo de cada corrida viaja como plantilla: Airflow la resuelve con las fechas de
    # ESA corrida (también en un backfill), y la tarea no mira el reloj para filtrar. Si se
    # dispara con los parámetros desde/hasta, reprocesa ese rango completo en una corrida.
    extraer_bronze = ExternalPythonOperator(
        task_id="extraer_bronze", python_callable=_extraer_bronze,
        op_kwargs={"env_file": ENV_FILE,
                   "inicio": "{{ params.desde or data_interval_start.isoformat() }}",
                   "fin": "{{ params.hasta or data_interval_end.isoformat() }}"},
        # Las fallas de URLhaus ya se reintentan adentro (3 intentos). Acá, un reintento más con
        # 10 min de espera, por si la caída fue más larga o se cayó el worker.
        retries=1, retry_delay=pendulum.duration(minutes=10),
        execution_timeout=pendulum.duration(hours=2),       # el reproceso baja la base completa
        **comunes)

    # Un test que falla es determinístico: volver a correrlo da lo mismo. Un solo reintento,
    # por si falló la lectura de MinIO.
    dbt_build = ExternalPythonOperator(
        task_id="dbt_build", python_callable=_dbt_build,
        op_kwargs={"env_file": ENV_FILE, "dbt_src": DBT_SRC, "dbt_run": DBT_RUN, "venv": VENV},
        retries=1, retry_delay=pendulum.duration(minutes=2), **comunes)

    # Si corta el control de volumen, reintentar da lo mismo; los reintentos son para una caída
    # de PostgreSQL.
    publicar_gold = ExternalPythonOperator(
        task_id="publicar_gold", python_callable=_publicar_gold,
        op_kwargs={"env_file": ENV_FILE, "dbt_run": DBT_RUN,
                   "aceptar_caida": "{{ params.aceptar_caida }}"}, **comunes)

    preparar_entorno >> [validar_postgres, extraer_bronze] >> dbt_build >> publicar_gold
