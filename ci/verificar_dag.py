"""Verifica que el DAG se importe en Airflow y tenga la forma esperada.

Falla (exit 1) si:
  * algún archivo de dags/ tiene errores de importación,
  * no aparece pipeline_grupo07,
  * faltan tareas o cambian las dependencias,
  * no hay reintentos en default_args, o el DAG no tiene schedule.

Uso: python ci/verificar_dag.py
"""
import sys
from pathlib import Path

from airflow.models import DagBag

DAGS = Path(__file__).resolve().parent.parent / "dags"
errores = []

bag = DagBag(dag_folder=str(DAGS), include_examples=False)
for archivo, error in bag.import_errors.items():
    errores.append(f"Error de importación en {archivo}:\n{error}")

dag = bag.dags.get("pipeline_grupo07")
if dag is None:
    errores.append("No se encontró el DAG pipeline_grupo07")
else:
    esperadas = {
        "preparar_entorno": set(),
        "validar_postgres": {"preparar_entorno"},
        "extraer_bronze": {"preparar_entorno"},
        "dbt_build": {"validar_postgres", "extraer_bronze"},
        "publicar_gold": {"dbt_build"},
    }
    tareas = {t.task_id: t for t in dag.tasks}
    if set(tareas) != set(esperadas):
        errores.append(f"Tareas: {sorted(tareas)}; se esperaban {sorted(esperadas)}")
    for tid, previas in esperadas.items():
        if tid in tareas and set(tareas[tid].upstream_task_ids) != previas:
            errores.append(f"{tid} depende de {sorted(tareas[tid].upstream_task_ids)}; "
                           f"se esperaba {sorted(previas)}")
    if not dag.default_args.get("retries"):
        errores.append("default_args no define retries (o están en 0)")
    if dag.timetable.summary in ("None", "") or dag.schedule_interval is None:
        errores.append("El DAG no tiene schedule")
    # extraer_bronze tiene que recibir el intervalo de Airflow (backfill), no mirar el reloj
    if "extraer_bronze" in tareas:
        kw = tareas["extraer_bronze"].op_kwargs
        if "data_interval_start" not in kw.get("inicio", "") or "data_interval_end" not in kw.get("fin", ""):
            errores.append("extraer_bronze no recibe data_interval_start / data_interval_end")
    # reproceso de un rango en una sola corrida (docs/BACKFILL.md)
    if not {"desde", "hasta", "aceptar_caida"} <= set(dag.params.keys()):
        errores.append("Faltan los parámetros del DAG (desde/hasta del reproceso, aceptar_caida del control de volumen)")

if errores:
    print("\n".join(f"❌ {e}" for e in errores))
    sys.exit(1)
print(f"✅ pipeline_grupo07 importa bien: {len(dag.tasks)} tareas, "
      f"schedule {dag.timetable.summary}, retries={dag.default_args['retries']}")
