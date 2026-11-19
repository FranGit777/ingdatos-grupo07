"""CI: extraer_bronze lee el export de URLhaus por nombre de columna, no por posición.

Sirve tres exports de prueba por HTTP local y corre la tarea real del DAG contra el S3
descartable del CI, en un bucket aparte (no toca el bronze de prueba de dbt):

1. Columnas en el orden estándar de URLhaus.
2. Las mismas URLs con las columnas en otro orden y una columna nueva: bronze tiene que
   quedar idéntico al caso 1 (y sin duplicar: es upsert por id).
3. Un export al que le falta una columna: la tarea tiene que fallar sin escribir nada.
"""
import functools
import http.server
import os
import shutil
import sys
import tempfile
import threading
import types
from datetime import datetime, timedelta, timezone

import boto3
from deltalake import DeltaTable

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUCKET = "pruebacolumnas"

# --- La función real del DAG, sin cargar Airflow (el job de dbt no lo instala)
for modulo in ["airflow", "airflow.models", "airflow.models.param", "airflow.operators",
               "airflow.operators.bash", "airflow.operators.python", "pendulum"]:
    sys.modules.setdefault(modulo, types.ModuleType(modulo))
fuente = open(os.path.join(RAIZ, "dags", "pipeline_grupo07.py"), encoding="utf-8").read()
fuente = fuente.split("# ---------------------------------------------------------------------------- DAG")[0]
fuente = "\n".join(l for l in fuente.splitlines() if not l.startswith(("from airflow", "import pendulum")))
espacio = {"__file__": os.path.join(RAIZ, "dags", "pipeline_grupo07.py")}
exec(compile(fuente, "pipeline_grupo07.py", "exec"), espacio)
extraer_bronze = espacio["_extraer_bronze"]

# --- Tres exports de prueba, con fechas de ayer (así la tarea usa el export de 30 días)
ayer = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
urls = [(9001, f"{ayer} 10:00:00", "http://a.invalid/1", "online", "", "malware_download",
         "Mozi,elf", "https://urlhaus.abuse.ch/url/9001/", "r1"),
        (9002, f"{ayer} 11:00:00", "http://a.invalid/2", "offline", "", "malware_download",
         "SmartLoader,zip", "https://urlhaus.abuse.ch/url/9002/", "r2")]
estandar = ["id", "dateadded", "url", "url_status", "last_online", "threat", "tags",
            "urlhaus_link", "reporter"]


def export(nombres, filas):
    """CSV como lo publica URLhaus: comentarios, encabezado comentado y filas entre comillas."""
    lineas = ["#" * 60, "# abuse.ch URLhaus Database Dump (CSV)", "# " + ",".join(nombres)]
    lineas += [",".join(f'"{v}"' for v in fila) for fila in filas]
    return "\n".join(lineas) + "\n"


desordenado = ["reporter", "url", "id", "columna_nueva", "tags", "threat", "dateadded",
               "urlhaus_link", "url_status", "last_online"]
pos = {n: i for i, n in enumerate(estandar)}
carpeta = tempfile.mkdtemp()
archivos = {
    "estandar.csv": export(estandar, urls),
    "desordenado.csv": export(desordenado, [[("x" if n == "columna_nueva" else fila[pos[n]])
                                             for n in desordenado] for fila in urls]),
    "sin_tags.csv": export([n for n in estandar if n != "tags"],
                           [[v for n, v in zip(estandar, fila) if n != "tags"] for fila in urls]),
}
for nombre, texto in archivos.items():
    open(os.path.join(carpeta, nombre), "w", encoding="utf-8").write(texto)
servidor = http.server.ThreadingHTTPServer(
    ("127.0.0.1", 0), functools.partial(http.server.SimpleHTTPRequestHandler, directory=carpeta))
threading.Thread(target=servidor.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{servidor.server_port}"

# --- Entorno: el S3 del CI, un bucket propio y una clave falsa
env = os.path.join(carpeta, ".env")
open(env, "w").write(f"MINIO_BUCKET={BUCKET}\nURLHAUS_AUTH_KEY=clave-falsa-del-ci\n")
boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT_URL"],
             region_name=os.environ.get("AWS_REGION", "us-east-1")).create_bucket(Bucket=BUCKET)
os.environ["MINIO_BUCKET"] = BUCKET
opciones = {"AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT_URL"], "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1")}
inicio = f"{ayer}T00:00:00+00:00"
fin = (datetime.fromisoformat(inicio) + timedelta(days=1)).isoformat()


def correr(archivo):
    shutil.rmtree("/tmp/urlhaus_grupo07", ignore_errors=True)   # sin la caché de 6 horas
    os.environ["URLHAUS_CSV_RECIENTE"] = f"{base}/{archivo}"
    return extraer_bronze(env, inicio, fin)


def bronze():
    t = DeltaTable(f"s3://{BUCKET}/bronze/series", storage_options=opciones).to_pandas()
    return t.sort_values("id").reset_index(drop=True)


errores = []
print("--- 1) Orden estándar"); n = correr("estandar.csv"); primero = bronze()
if n != 2:
    errores.append(f"Con el orden estándar se escribieron {n} URLs en lugar de 2")
if list(primero["id"]) != [9001, 9002] or list(primero["reporter"]) != ["r1", "r2"]:
    errores.append(f"Orden estándar: bronze quedó mal\n{primero}")

print("--- 2) Columnas en otro orden y una nueva"); n = correr("desordenado.csv"); segundo = bronze()
if n != 2:
    errores.append(f"Con las columnas desordenadas se escribieron {n} URLs en lugar de 2")
if not segundo.equals(primero):
    errores.append(f"Con las columnas desordenadas bronze cambió:\n{primero}\n{segundo}")

print("--- 3) Falta la columna tags")
try:
    correr("sin_tags.csv")
    errores.append("Con una columna faltante la tarea no falló")
except RuntimeError as e:
    print(f"Falló como se esperaba: {e}")
if not bronze().equals(primero):
    errores.append("Con una columna faltante se escribió algo en bronze")

servidor.shutdown()
if errores:
    print("\n".join(errores)); sys.exit(1)
print("OK: extraer_bronze lee las columnas por nombre (orden distinto, columna nueva, columna faltante)")
