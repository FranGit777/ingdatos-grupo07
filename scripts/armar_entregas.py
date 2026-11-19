"""Arma un .zip por entrega para subir al campus: el README de la entrega y su código.

Uso (desde la raíz del repo, después de correr los notebooks para que tengan las salidas):
    python scripts/armar_entregas.py            # las cuatro
    python scripts/armar_entregas.py E1 E3      # algunas

Los zips quedan en entregas_zip/ (no se versiona). Solo entran archivos que están en Git,
así que nunca se cuela un .env, una clave o un warehouse.duckdb.
"""
import re
import subprocess
import sys
import zipfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
SALIDA = RAIZ / "entregas_zip"

# Entrega -> (README de la entrega, archivos o carpetas que la acompañan)
ENTREGAS = {
    "E1": ("docs/entregas/E1_ingesta.md",
           ["notebooks/NB02_semana04_extraccion_bronze.ipynb", "entorno.py",
            "requirements.txt", ".env.example"]),
    "E2": ("docs/entregas/E2_dbt.md",
           ["dbt", "notebooks/NB03_semana07_duckdb_delta_lake.ipynb",
            "notebooks/NB04_semana08_dbt_silver_gold.ipynb", "docs/linaje.png",
            "scripts/linaje.py", "entorno.py", "requirements.txt"]),
    "E3": ("docs/entregas/E3_airflow_ci.md",
           ["dags/pipeline_grupo07.py", "notebooks/NB05_semana10_airflow_dag.ipynb", "dbt",
            ".github/workflows/ci.yml", "ci", "docs/BACKFILL.md",
            "docs/INCIDENTE_2026-11-18_backfill.md", "scripts/recuperar_bronze.py",
            "entorno.py", "requirements.txt", ".env.example"]),
    "E4": ("docs/entregas/E4_serving_governance.md",
           ["notebooks/NB06_semana14_serving_metabase_grupo07.ipynb", "docs/GOVERNANCE.md",
            "docs/TABLERO.md", "docs/tablero.png", "docs/linaje.png", "entorno.py",
            "requirements.txt"]),
}


def versionados():
    salida = subprocess.run(["git", "ls-files"], cwd=RAIZ, capture_output=True, text=True, check=True)
    return set(salida.stdout.splitlines())


def readme(ruta):
    """El README de la entrega va a la raíz del zip: se ajustan sus links relativos."""
    texto = (RAIZ / ruta).read_text(encoding="utf-8")
    texto = re.sub(r"\]\(\.\./\.\./", "](", texto)        # ../../notebooks/x -> notebooks/x
    texto = re.sub(r"\]\(\.\./", "](docs/", texto)         # ../GOVERNANCE.md  -> docs/GOVERNANCE.md
    return texto


def armar(clave, en_git):
    doc, piezas = ENTREGAS[clave]
    archivos = sorted(f for f in en_git
                      if any(f == p or f.startswith(p.rstrip("/") + "/") for p in piezas))
    faltan = [p for p in piezas if not any(f == p or f.startswith(p + "/") for f in archivos)]
    if faltan:
        sys.exit(f"{clave}: no están en Git {faltan}")
    SALIDA.mkdir(exist_ok=True)
    destino = SALIDA / f"grupo07_{clave}.zip"
    carpeta = f"grupo07_{clave}"
    with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{carpeta}/README.md", readme(doc))
        for f in archivos:
            z.write(RAIZ / f, f"{carpeta}/{f}")
    print(f"{destino.relative_to(RAIZ)}: README.md + {len(archivos)} archivos")


if __name__ == "__main__":
    pedidas = [a.upper() for a in sys.argv[1:]] or list(ENTREGAS)
    en_git = versionados()
    for clave in pedidas:
        armar(clave, en_git)
