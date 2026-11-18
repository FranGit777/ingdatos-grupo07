"""Recupera en bronze las URLs que borró el backfill del 18/11/2026.

Contexto en docs/INCIDENTE_2026-11-18_backfill.md. Fuente de la recuperación: una copia del
warehouse.duckdb que generó NB04 ANTES del backfill; su tabla stg_urlhaus (silver) tiene todas
las URLs que había en bronze en ese momento.

Qué hace: arma, desde esa silver, filas con el esquema de bronze y las agrega con un merge por
id que SOLO inserta las URLs que faltan. Las que ya están en bronze no se tocan (son las más
nuevas). Correrlo dos veces no cambia nada la segunda vez.

Lo que no se puede recuperar desde silver: last_online (silver no lo guardaba) queda vacío;
urlhaus_link se reconstruye con el formato fijo de URLhaus a partir del id.

Uso, desde ~/work en la terminal del JupyterHub:
    python scripts/recuperar_bronze.py _viejos/warehouse_antes_backfill.duckdb            # solo muestra
    python scripts/recuperar_bronze.py _viejos/warehouse_antes_backfill.duckdb --aplicar  # escribe
"""
import argparse
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from entorno import BRONZE, STORAGE_OPTIONS  # noqa: E402

ESQUEMA = pa.schema([
    ("id", pa.int64()), ("date_added", pa.timestamp("us", tz="UTC")),
    ("url", pa.string()), ("url_status", pa.string()), ("last_online", pa.string()),
    ("threat", pa.string()), ("tags", pa.list_(pa.string())),
    ("urlhaus_link", pa.string()), ("reporter", pa.string()),
])

parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
parser.add_argument("duckdb", help="copia de warehouse.duckdb de antes del backfill")
parser.add_argument("--aplicar", action="store_true", help="escribir en bronze (sin esto, solo muestra)")
args = parser.parse_args()

# 1) Silver de antes del backfill -> filas con el esquema de bronze
con = duckdb.connect(args.duckdb, read_only=True)
con.sql("set TimeZone = 'UTC'")
silver = con.sql("""
    select
        cast(id_amenaza as bigint)                                     as id,
        cast(fecha_agregado as timestamp) at time zone 'UTC'           as date_added,
        url_maliciosa                                                  as url,
        estado                                                         as url_status,
        cast(null as varchar)                                          as last_online,
        tipo_amenaza                                                   as threat,
        -- etiquetas: '[Mozi, elf]' (lista pasada a texto) o 'Mozi,elf' -> lista
        list_filter(list_transform(
            string_split(regexp_replace(coalesce(etiquetas, ''), '[\\[\\]''"]', '', 'g'), ','),
            x -> trim(x)), x -> x <> '')                               as tags,
        'https://urlhaus.abuse.ch/url/' || id_amenaza || '/'           as urlhaus_link,
        reportero                                                      as reporter
    from stg_urlhaus
""").arrow()
con.close()
if not isinstance(silver, pa.Table):          # duckdb >= 1.4 devuelve un RecordBatchReader
    silver = silver.read_all()
tags = pc.if_else(pc.equal(pc.list_value_length(silver["tags"]), 0),
                  pa.scalar(None, pa.list_(pa.string())), silver["tags"])
silver = silver.set_column(silver.schema.get_field_index("tags"), "tags", tags).cast(ESQUEMA)

# 2) Cuáles faltan en bronze
dt = DeltaTable(BRONZE, storage_options=STORAGE_OPTIONS)
ids_bronze = dt.to_pyarrow_table(columns=["id"])["id"]
faltan = silver.filter(pc.invert(pc.is_in(silver["id"], value_set=ids_bronze)))
print(f"Silver de antes del backfill: {silver.num_rows} URLs · bronze hoy: {len(ids_bronze)} · faltan: {faltan.num_rows}")

conteo = pc.value_counts(pc.strftime(faltan["date_added"], format="%Y-%m")).to_pylist()
for item in sorted(conteo, key=lambda x: x["values"]):
    print(f"  {item['values']}: {item['counts']} URLs a recuperar")

if not args.aplicar:
    print("\nNo se escribió nada. Para aplicarlo: agregar --aplicar")
    sys.exit(0)
if faltan.num_rows == 0:
    print("No falta nada.")
    sys.exit(0)

# 3) Insertar solo las que faltan (las que ya están no se tocan)
(dt.merge(source=faltan, predicate="t.id = s.id", source_alias="s", target_alias="t")
   .when_not_matched_insert_all()
   .execute())
DeltaTable(BRONZE, storage_options=STORAGE_OPTIONS).vacuum(
    retention_hours=0, enforce_retention_duration=False, dry_run=False)
total = DeltaTable(BRONZE, storage_options=STORAGE_OPTIONS).to_pyarrow_table(columns=["id"]).num_rows
print(f"\n✅ Recuperadas {faltan.num_rows} URLs. Bronze ahora: {total} URLs.")
