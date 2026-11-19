"""Dibuja el linaje del pipeline en docs/linaje.png a partir de los ref() de los modelos dbt.

Uso: python scripts/linaje.py   (necesita graphviz: el comando `dot`)
"""
import re
import subprocess
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
MODELOS = RAIZ / "dbt" / "models"
CAPA = {"stg_": ("silver", "#e7f1fb"), "fct_": ("gold", "#f9eeda")}

aristas, nodos = [], []
for sql in sorted(MODELOS.glob("*.sql")):
    modelo = sql.stem
    nodos.append(modelo)
    texto = sql.read_text(encoding="utf-8")
    for padre in re.findall(r"ref\('([a-z_]+)'\)", texto):
        aristas.append((padre, modelo))
    if "read_parquet" in texto:
        aristas.append(("bronze", modelo))
# publicar_gold copia a PostgreSQL las tablas gold
for m in nodos:
    if m.startswith("fct_"):
        aristas.append((m, "postgres"))

lineas = [
    "digraph linaje {",
    '  rankdir=LR; bgcolor="white"; nodesep=0.4; ranksep=0.7;',
    '  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=12, color="#555555"];',
    '  edge [color="#777777", arrowsize=0.7];',
    '  bronze [label="bronze\\nDelta Lake en MinIO\\ns3://grupo07/bronze/series", fillcolor="#f0efec"];',
    '  postgres [label="PostgreSQL\\nwarehouse.grupo07\\n(publicar_gold) → Metabase", fillcolor="#e5f4e4"];',
]
for m in nodos:
    capa, color = next(v for k, v in CAPA.items() if m.startswith(k))
    lineas.append(f'  {m} [label="{m}\\n({capa})", fillcolor="{color}"];')
lineas += [f"  {a} -> {b};" for a, b in aristas] + ["}"]

subprocess.run(["dot", "-Tpng", "-Gdpi=150", "-o", str(RAIZ / "docs" / "linaje.png")],
               input="\n".join(lineas), text=True, check=True)
print(f"docs/linaje.png: {len(nodos)} modelos, {len(aristas)} relaciones")
