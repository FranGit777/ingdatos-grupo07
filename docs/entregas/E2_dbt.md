# Entrega 2 — Transformación con dbt y calidad de datos

**Proyecto dbt:** [`dbt/`](../../dbt) (dbt-duckdb). Lo genera NB05 (secciones 3b y 3c) y lo corre NB04.
**Lee de:** el bronze real, la tabla Delta `s3://grupo07/bronze/series` en MinIO (`read_parquet('s3://grupo07/bronze/series/*.parquet')` en `stg_urlhaus`). No hay seeds ni CSV.

```
bronze (Delta, MinIO) ─▶ stg_urlhaus (silver) ─▶ fct_familias_mensual (gold) ─▶ fct_amenazas_mensual (gold)
                                                          └──────────────────▶ fct_features_familias (E4)
```

## 1. Cómo correr el build desde cero

En el JupyterHub, con el repo en `~/work` (ver [E1](E1_ingesta.md)) y bronze con datos:

- **Desde el notebook:** abrir `notebooks/NB04_semana08_dbt_silver_gold.ipynb` y correr todo. Corre `dbt build` (modelos y tests), muestra silver contra gold y genera la documentación con el linaje.
- **Desde la terminal:**
  ```bash
  cd ~/work && pip install -q -r requirements.txt
  cd dbt
  dbt build --profiles-dir .                    # 4 modelos + 29 tests
  dbt docs generate --static --profiles-dir .   # linaje en target/static_index.html
  ```

Las credenciales de MinIO las toma `profiles.yml` de las variables de entorno del JupyterHub; no hay nada escrito en el proyecto.

## 2. Qué limpia silver (`stg_urlhaus`)

Una fila de silver es **una URL maliciosa reportada a URLhaus**, una sola vez. Estos son los problemas que encontramos en los datos y qué decidimos con cada uno:

| Problema encontrado | Dónde lo vimos | Qué hace silver |
|---|---|---|
| `threat` vale `malware_download` en el 100 % de las filas: no sirve para distinguir amenazas | NB03, exploración | Deduce dos dimensiones nuevas de las etiquetas: **familia de malware** y **tipo de archivo** |
| Las etiquetas mezclan la familia con ruido: tipo de archivo (`elf`, `exe`), arquitectura (`32-bit`, `mips`), hosting (`github`, `discord`), país (`CHN`), `dropped-by-*`, `ua-*` | NB03 | La familia es la primera etiqueta que no es nada de eso |
| Con datos reales aparecieron como "familias" IPs escritas con guiones (`176-65-139-204`, a veces con puerto), dominios (`pw-dvx.to`, `bauble-cleverpondky-com`), honeypots y protocolos (`cowrie`, `ssh`), términos genéricos (`botnet`, `iot`, `docker`, `malware-download`) y pedazos de hash (`d52f85`) | Tabla de features, al revisar las familias más activas de octubre | Se excluyen con dos patrones (IP y dominio), una lista de genéricos y un patrón de hash. Verificado contra los 30 nombres reales con guiones y contra 29 familias reales: ningún falso positivo |
| La misma familia escrita distinto (`Mozi` / `mozi`, `Mirai` / `mirai`) | NB03 | Se unifica con la grafía más frecuente |
| Las fechas se corrían 3 horas según la máquina: `cast(date_added as timestamp)` usa la zona horaria de la sesión, y con hora argentina las URLs de las últimas 3 horas de cada mes caían en el mes anterior | El bronze de prueba del CI, al comparar los conteos por mes | `profiles.yml` fija `TimeZone: UTC` y un test verifica que dbt corra en UTC |
| La misma URL puede llegar más de una vez (reingestas, la carga histórica y la diaria se superponen) | Diseño de la ingesta | Deduplica por `id` y se queda con la observación más reciente |
| `tags` llega como texto (`"Mozi,elf"`) desde el CSV y como lista desde la API | NB02 y DAG | Se normaliza a texto y se separa en silver |
| Filas sin `id` o sin fecha | Defensivo | Se descartan |

**Lo que decidimos dejar:** la URL maliciosa y el usuario que la reportó (`reportero`) quedan en silver, para poder auditar, pero no pasan a gold ni a PostgreSQL (ver la clasificación en `GOVERNANCE.md`). La familia es aproximada y lo decimos: las etiquetas las carga cada reportero sin un formato fijo, y siempre puede aparecer un tag nuevo que no es familia.

**Un solo modelo silver:** el dominio tiene una sola entidad, la URL reportada. URLhaus no publica un catálogo de familias ni de reporteros con atributos propios que justifique una dimensión aparte: familia y tipo de archivo son atributos de cada URL. La consigna lo admite si el dominio es simple, y preferimos uno bien limpio a dos por cumplir.

**Tabla y no vista:** silver se materializa como tabla porque la leen la gold atómica y ocho tests. Como vista, cada uno volvería a leer todos los `.parquet` de MinIO; materializada, se lee una vez por build. Las gold también son tablas: es lo que se publica y lo que lee Metabase.

## 3. Qué pregunta responde gold

> **¿Qué familias de malware son las más activas y en qué formato de archivo se distribuyen, mes a mes?**

- **`fct_familias_mensual`** (gold atómica): URLs nuevas por mes × familia × tipo de archivo. Unas 2.000 filas contra las casi 190.000 de silver.
- **`fct_amenazas_mensual`** (gold agregada): URLs nuevas por mes, para la pregunta "¿sube o baja el volumen?". Se calcula sumando la atómica, así que las dos nunca pueden contradecirse (diseño Kimball: un hecho atómico y un agregado derivado).

Las columnas están documentadas en `schema.yml`, en lenguaje de negocio, y se ven en `dbt docs` y como comentarios en PostgreSQL.

## 4. Tests

**Genéricos** (`schema.yml`): `not_null` en las claves y dimensiones de cada modelo, `unique` en `id_amenaza` y `accepted_values` del mes (1 a 12). No van en todas las columnas: van donde un nulo o un duplicado rompería un conteo.

**Propios del dominio** (`dbt/tests/`):

| Test | Qué detecta | Problema real |
|---|---|---|
| `assert_gold_cuadra` | Silver, las dos gold y los features no suman la misma cantidad de URLs | Una URL contada dos veces o perdida en una agregación |
| `assert_fechas_validas` | URLs registradas antes de 2018 (cuando arrancó URLhaus) o en el futuro | Un error de parseo o de zona horaria en la ingesta |
| `assert_familia_no_es_tag_generico` | Una arquitectura, extensión, hosting, país, IP, dominio, honeypot o hash contado como familia | **Falló de verdad:** con el silver anterior detectó las IPs y dominios que aparecían como familias en el tablero; lo usamos para validar el arreglo |
| `assert_zona_horaria_utc` | dbt corriendo con una zona horaria que no es UTC | El corrimiento de 3 horas que movía URLs de fin de mes al mes anterior |
| `assert_features_sin_fuga` y `assert_features_consistentes` | Features que usan meses futuros, o fuera de rango (E4) | **`assert_features_consistentes` falló en producción:** en los meses sin URLs, la participación daba 0/0 = NaN, y DuckDB considera NaN mayor que 100. 359 filas. Se corrigió y el CI ahora incluye meses vacíos |

Si un test falla, `dbt build` termina con error, el DAG no publica y PostgreSQL conserva la última versión buena.

## 5. Linaje

`dbt docs generate --static` (sección 4 de NB04) arma el sitio con las descripciones y el grafo de linaje en `dbt/target/static_index.html`. Captura en [`docs/linaje.png`](../linaje.png).

## 6. Para la demo (10 minutos)

1. **Build en verde:** NB04 sección 2 (o `dbt build` en la terminal): 4 modelos y 33 tests en PASS.
2. **Un test fallando:** el pull request de demostración [#24](https://github.com/FranGit777/ingdatos-grupo07/pull/24) saca el patrón de IPs de silver; el CI corre `dbt build` sobre el bronze de prueba y `assert_familia_no_es_tag_generico` falla porque `176-65-139-204` pasa a contarse como familia. El PR queda bloqueado.
3. **Linaje:** abrir `static_index.html` y mostrar el grafo bronze → silver → gold → features.
