# Governance del dataset — Amenazas URLhaus (grupo07)

Ingeniería de Datos — LCD · UCA Rosario · 2026 · grupo07

## 1. Identificación

| Campo | Valor |
| --- | --- |
| Nombre | Amenazas URLhaus mensuales |
| Tablas publicadas | `warehouse.grupo07.fct_familias_mensual` (gold atómica), `warehouse.grupo07.fct_amenazas_mensual` (gold agregada), `warehouse.grupo07.fct_features_familias` (features por mes y familia) |
| Responsable | grupo07 (Ingeniería de Datos, LCD UCA Rosario 2026) |
| Propósito | Seguir la evolución mensual de URLs que distribuyen malware, por familia de malware y tipo de archivo, para priorizar detección y prevención |
| Consumidores | Tablero "Amenazas URLhaus — grupo07" en Metabase |
| Grano | `fct_familias_mensual`: una fila por (mes, tipo de amenaza, familia, tipo de archivo). `fct_amenazas_mensual`: una fila por (mes, tipo de amenaza) |
| Frecuencia | Diaria (`@daily`), DAG `pipeline_grupo07` en Airflow. `actualizado_en` indica la última publicación |

## 2. Origen

- **Fuente:** URLhaus, de abuse.ch. Es una base pública de URLs que distribuyen malware, cargada por investigadores de seguridad.
- **Extracción:** volcados CSV de URLhaus (últimos 30 días o base completa), con Auth-Key propia. Cada corrida de Airflow toma solo las URLs registradas en su intervalo `[data_interval_start, data_interval_end)`, así que la ingesta es reproducible y admite backfill.
- **Linaje:**
  1. `extraer_bronze` → bronze en Delta Lake (`s3://grupo07/bronze/series`, MinIO), una fila por URL, tal como la publica URLhaus.
  2. dbt `stg_urlhaus` (silver) → tipado, deduplicado por id, y derivación de `familia_malware` y `tipo_archivo` a partir de los tags.
  3. dbt `fct_familias_mensual` (gold atómica) → conteo por mes, tipo de amenaza, familia y tipo de archivo.
  4. dbt `fct_amenazas_mensual` (gold agregada) → suma de la atómica por mes y tipo de amenaza.
  5. dbt `fct_features_familias` (features) → por mes y familia: mes anterior, variación, promedio móvil de 3 meses, acumulado y participación, mirando solo hacia atrás.
  6. `publicar_gold` → PostgreSQL (`warehouse.grupo07`), las tres tablas con reemplazo total en una sola transacción, y de ahí a Metabase.

## 3. Diccionario

**`fct_familias_mensual`** (gold atómica)

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `fecha` | date | Primer día del mes en que URLhaus registró la URL |
| `anio` | integer | Año de registro |
| `mes` | integer | Mes de registro (1–12) |
| `tipo_amenaza` | text | Campo `threat` de URLhaus (en la práctica, `malware_download`) |
| `familia_malware` | text | Primer tag que nombra una familia de malware, con las mayúsculas unificadas. `sin_familia` si no hay |
| `tipo_archivo` | text | Primer tag de tipo de archivo (`elf`, `exe`, `zip`, `sh`…). `desconocido` si no hay |
| `cantidad_amenazas` | bigint | URLs distintas (por id) registradas en el mes con esa combinación |
| `actualizado_en` | timestamptz | Momento de la publicación en PostgreSQL |

Clave primaria: (`anio`, `mes`, `tipo_amenaza`, `familia_malware`, `tipo_archivo`).

**`fct_amenazas_mensual`** (gold agregada): tiene `anio`, `mes`, `tipo_amenaza`, `cantidad_amenazas` y `actualizado_en`, con el mismo significado que arriba. Clave primaria: (`anio`, `mes`, `tipo_amenaza`).

**`fct_features_familias`** (features): una fila por (`fecha`, `familia_malware`), con `cantidad_amenazas`, `cantidad_mes_anterior`, `variacion_mensual_pct`, `promedio_movil_3m`, `acumulado`, `pct_del_mes` y `mes_completo`. Los meses sin URLs de una familia valen 0. Ningún valor usa meses posteriores al de la fila (test `assert_features_sin_fuga`).

Cada columna también tiene su descripción como `COMMENT` en PostgreSQL, y Metabase la muestra.

## 4. Sensibilidad

- **Clasificación:** dato público de inteligencia de amenazas. No contiene datos personales de víctimas.
- **Minimización:** a PostgreSQL y a Metabase solo llegan datos agregados. Las URLs maliciosas, los tags crudos y el usuario de URLhaus que reportó cada URL (`reporter`) quedan solo en bronze y silver.
- **Riesgo:** las URLs de bronze y silver apuntan a malware activo. No se publican como enlaces clicables ni se abren desde el tablero.
- **Credenciales:** la Auth-Key de URLhaus y las contraseñas de MinIO y PostgreSQL viven en variables de entorno y en `dags/.env`, que no se versiona. NB05 pide la API key con `getpass`, así que no queda escrita en ningún notebook. Las tareas leen las credenciales adentro, así que no aparecen en "Rendered Template" de Airflow.
- **Acceso:** Metabase lee con el usuario del grupo, y solo el esquema `grupo07`.

## 5. Calidad

| Control | Dónde | Qué detecta |
| --- | --- | --- |
| `not_null` + `unique` en `id_amenaza` | dbt, silver | URLs duplicadas o sin id |
| `not_null` en fechas, familia, tipo de archivo y claves | dbt, silver y gold | Filas incompletas |
| `accepted_values` de `mes` (1–12) | dbt, gold | Meses mal calculados |
| `assert_fechas_validas` (test propio) | dbt, silver | Fechas anteriores a 2018 o futuras: errores de parseo o de zona horaria |
| `assert_familia_no_es_tag_generico` (test propio) | dbt, silver | Una arquitectura, extensión, hosting o país contado como familia |
| `assert_gold_cuadra` (test propio) | dbt, gold | Silver y las dos gold que no suman lo mismo |
| Clave primaria y `check (mes between 1 and 12)` | PostgreSQL | Duplicados o meses inválidos al publicar |
| No publicar una gold vacía y verificar el conteo | DAG `publicar_gold` | Publicaciones incompletas |
| `assert_features_sin_fuga` (test propio) | dbt, features | Un feature que usa meses posteriores (fuga temporal) |
| `assert_features_consistentes` (test propio) | dbt, features | Grano duplicado, valores negativos o participación fuera de 0–100 % |
| `assert_zona_horaria_utc` (test propio) | dbt | dbt corriendo fuera de UTC (correría URLs de fin de mes al mes anterior) |
| Las tres tablas suman lo mismo | DAG `publicar_gold` y NB06 | Publicación incompleta o desincronizada |

Si un test de dbt falla, `dbt_build` falla y `publicar_gold` no corre: PostgreSQL conserva la última versión buena.

**Limitaciones conocidas:**
- La familia es aproximada, porque los tags los cargan quienes reportan, sin un formato fijo.
- `fecha` es cuándo URLhaus registró la URL, no cuándo ocurrió el ataque.
- El mes en curso está incompleto.

## 6. Licencia

- **Datos de URLhaus:** abuse.ch los publica para uso libre, incluido el comercial, bajo CC0 (dominio público). El acceso a la API y a los volcados (`urlhaus-api.abuse.ch/v2/files/exports/`) requiere una Auth-Key gratuita. Hay que citar a URLhaus/abuse.ch como fuente. *Verificar los términos vigentes en https://urlhaus.abuse.ch/ al momento de la entrega.*
- **Datos derivados (silver, gold, features):** se publican bajo los mismos términos que la fuente, citando URLhaus/abuse.ch.
- **Código del pipeline:** desarrollado por grupo07 para la materia Ingeniería de Datos (LCD UCA Rosario 2026), con fines académicos.
