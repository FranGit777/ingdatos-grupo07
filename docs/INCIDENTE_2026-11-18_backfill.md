# Incidente 18/11/2026 — el backfill borró URLs de bronze

**Estado:** causa corregida. Recuperación de datos en curso (ver *Recuperación*).

## Qué pasó

Para completar el histórico de bronze, que antes de julio de 2026 tenía pocas URLs por mes, se disparó el DAG con `desde = 2018-01-01` y `hasta = 2026-11-19`. La corrida terminó en verde (33 tests en PASS), pero varios meses quedaron con **menos** URLs que antes:

| Mes | Antes del backfill | Después |
|---|---:|---:|
| jun 2026 | 4.734 | 3.632 |
| jul 2026 | 4.580 | 124 |
| ago 2026 | 15.663 | 4.347 |
| sep 2026 | 15.203 | 15.198 |
| oct 2026 | 44.780 | 121.958 |

Octubre se completó (≈ 4.000 URLs por día, lo esperable para URLhaus); junio a agosto perdieron datos.

## Causa

`extraer_bronze` **reemplazaba** en bronze las filas del intervalo procesado (`overwrite` con `predicate` por fecha). Eso solo es correcto si la fuente trae, para esas fechas, todas las URLs. El export `full.csv.zip` de URLhaus no lo hace: trae casi todo el último mes y, de los meses anteriores, solo una parte. Al reemplazar cada mes con lo que traía el export, bronze perdió las URLs que habíamos acumulado y que el origen ya no publica.

Lo habíamos anotado como límite en `BACKFILL.md` ("trae lo que URLhaus tiene hoy"), pero no se verificó con datos reales cuánto faltaba antes de correr el reproceso sobre todo el histórico.

**Por qué los tests no lo detectaron:** verifican que los datos sean consistentes entre sí (sin duplicados, fechas válidas, silver y gold que cuadran), y lo eran. Ningún test compara la cantidad de URLs de un mes contra la corrida anterior.

**Por qué no se pudo volver atrás con Delta:** `extraer_bronze` hace `vacuum` con retención 0 después de escribir (para que dbt, que lee los `.parquet` directamente, no lea versiones viejas). Eso borra los archivos de las versiones anteriores, así que el *time travel* no tenía a dónde volver.

## Corrección

1. **Bronze ya no borra.** `extraer_bronze` hace upsert por `id` (`merge`): la URL que ya está se actualiza, la nueva se agrega, y la que el origen dejó de publicar se conserva. Sigue siendo idempotente. Probado con un segundo export al que le faltan 775 URLs: bronze no pierde ninguna y actualiza los estados que cambiaron.
2. **Recuperación de lo borrado** con `scripts/recuperar_bronze.py`, a partir de la silver de una copia de `warehouse.duckdb` generada por NB04 a las 13:29, antes del backfill. Solo inserta las URLs que faltan en bronze; no toca las que ya están. Lo que no se puede recuperar desde silver: `last_online`, que queda vacío en esas filas.

## Recuperación

Desde `~/work` en el JupyterHub:

```bash
python scripts/recuperar_bronze.py _viejos/warehouse_antes_backfill.duckdb            # muestra qué recuperaría
python scripts/recuperar_bronze.py _viejos/warehouse_antes_backfill.duckdb --aplicar  # lo aplica
```

Después, correr el DAG (sin parámetros) para recalcular silver y gold y republicar.

**Segundo tropiezo, durante la recuperación:** el primer intento del script falló sin escribir nada (un `merge` de Delta que falla no deja cambios a medias): en el servidor la columna `tags` de bronze es **texto** (`"Mozi,elf"`, como en el CSV de URLhaus), y el script y el DAG suponían una **lista**. En las pruebas no apareció porque ahí bronze se creaba desde cero con el esquema supuesto. Se corrigió en dos niveles: `tags` pasa a ser texto en todo el pipeline (es como lo publica la fuente; silver ya lo separaba), y el DAG, NB02 y el script **adaptan cada lote al esquema real de bronze** antes de escribir. El bronze de prueba del CI ahora usa el mismo esquema que producción.

## Qué aprendimos

- Las pruebas tienen que usar el **mismo esquema que producción**: un bronze de prueba creado con el esquema que creemos tener no detecta las diferencias con el que realmente hay.
- La capa cruda **acumula**: nunca se reemplaza con lo que trae la fuente hoy, porque la fuente puede olvidar.
- Antes de un reproceso grande hay que medir qué trae la fuente para ese período, por ejemplo URLs por mes, y compararlo con lo que ya hay.
- `vacuum` con retención 0 nos dejó sin red. Si dbt leyera la tabla Delta con `delta_scan` en vez de los `.parquet` sueltos, se podría conservar historia de versiones (pendiente).
