"""Carga un bronze chico de prueba en S3 (MinIO o moto) para correr dbt en el CI.

Mismo esquema y misma ruta que escribe `extraer_bronze` del DAG
(s3://<MINIO_BUCKET>/bronze/series, Delta Lake). Las filas están armadas para
ejercitar lo que limpia silver:

  * un id repetido (deduplicación por id),
  * la misma familia con distintas mayúsculas (Mozi / mozi),
  * tags genéricos, de hosting, de país, dropped-by-* y ua-* que NO son familia,
  * URLs sin tags (sin_familia / desconocido),
  * IPs y dominios usados como tag (176-65-139-204, pw-dvx.to, bauble-cleverpondky-com),
  * meses sin ninguna URL (sep y oct 2025), como los huecos del histórico real.

Uso: python ci/bronze_de_prueba.py
"""
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
from deltalake import write_deltalake

BUCKET = os.environ.get("MINIO_BUCKET", "grupo07")
ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
OPCIONES = {
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    "AWS_ENDPOINT_URL": ENDPOINT,
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


def utc(texto):
    return datetime.fromisoformat(texto).replace(tzinfo=timezone.utc)


#        id    date_added             status     tags
FILAS = [
    (1000, "2025-08-15 09:00:00", "offline", ["AsyncRAT", "exe"]),  # después, 2 meses vacíos
    (1001, "2025-11-03 10:00:00", "online",  ["Mozi", "elf", "32-bit"]),
    (1002, "2025-11-04 11:30:00", "offline", ["mozi", "elf", "mips"]),      # misma familia, otra grafía
    (1003, "2025-11-20 08:15:00", "offline", ["Mozi", "sh"]),
    (1004, "2025-12-01 00:00:00", "online",  ["32-bit", "elf", "mirai"]),   # la familia no es el primer tag
    (1005, "2025-12-15 13:45:00", "offline", ["github", "exe", "AsyncRAT"]),
    (1006, "2025-12-31 23:59:59", "offline", ["CHN", "zip", "LuaJIT-loader"]),
    (1007, "2026-01-02 06:00:00", "online",  ["dropped-by-SmartLoader", "exe"]),  # no tiene familia
    (1008, "2026-01-10 09:00:00", "offline", ["ua-wget", "sh"]),            # no tiene familia
    (1009, "2026-01-11 17:20:00", "offline", None),                          # sin tags
    (1011, "2026-02-06 10:00:00", "online",  ["176-65-139-204", "elf", "mirai"]),  # la IP no es familia
    (1012, "2026-02-06 11:00:00", "online",  ["pw-dvx.to", "cowrie", "ssh", "exe"]),  # ni el dominio ni el honeypot
    (1013, "2026-02-07 12:00:00", "offline", ["bauble-cleverpondky-com", "176-65-134-121-8080", "LuaJIT-loader"]),
    (1010, "2026-02-05 12:00:00", "online",  ["opendir", "mirai", "arm7"]),
    (1010, "2026-02-05 12:00:00", "online",  ["opendir", "mirai", "arm7"]),  # id repetido
]

ESQUEMA = pa.schema([
    ("id", pa.int64()), ("date_added", pa.timestamp("us", tz="UTC")),
    ("url", pa.string()), ("url_status", pa.string()), ("last_online", pa.string()),
    ("threat", pa.string()), ("tags", pa.list_(pa.string())),
    ("urlhaus_link", pa.string()), ("reporter", pa.string()),
])

tabla = pa.table({
    "id": [f[0] for f in FILAS],
    "date_added": [utc(f[1]) for f in FILAS],
    "url": [f"http://ejemplo.invalid/{f[0]}" for f in FILAS],
    "url_status": [f[2] for f in FILAS],
    "last_online": [None] * len(FILAS),
    "threat": ["malware_download"] * len(FILAS),
    "tags": [f[3] for f in FILAS],
    "urlhaus_link": [f"https://urlhaus.abuse.ch/url/{f[0]}/" for f in FILAS],
    "reporter": ["ci"] * len(FILAS),
}, schema=ESQUEMA)

s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                  aws_access_key_id=OPCIONES["AWS_ACCESS_KEY_ID"],
                  aws_secret_access_key=OPCIONES["AWS_SECRET_ACCESS_KEY"],
                  region_name=OPCIONES["AWS_REGION"])
if BUCKET not in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
    s3.create_bucket(Bucket=BUCKET)

write_deltalake(f"s3://{BUCKET}/bronze/series", tabla, mode="overwrite", storage_options=OPCIONES)
print(f"Bronze de prueba: {tabla.num_rows} filas en s3://{BUCKET}/bronze/series")
