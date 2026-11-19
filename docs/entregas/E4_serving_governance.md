# Entrega 4 — Serving, features y governance

Con esta entrega los datos hacen el recorrido completo: URLhaus → bronze → silver → gold y features → PostgreSQL → tablero de Metabase.

## 1. Publicación de la gold en PostgreSQL

**Es la última tarea del DAG**, `publicar_gold`, y corre todos los días después de `dbt_build`. Nadie tiene que correr un notebook para que el warehouse se actualice. Publica tres tablas en `warehouse.grupo07`:

| Tabla | Grano | Para qué |
| --- | --- | --- |
| `fct_familias_mensual` | mes × familia × tipo de archivo | Rankings de familias y tipos de archivo |
| `fct_amenazas_mensual` | mes × tipo de amenaza | Volumen total por mes |
| `fct_features_familias` | mes × familia | Tendencia de cada familia (features) |

**Es repetible: correrla dos veces deja lo mismo.** Las tres tablas se reemplazan (`TRUNCATE` + `INSERT`) dentro de **una sola transacción**: si algo falla en el medio, PostgreSQL vuelve a la versión anterior completa de las tres. Al terminar, verifica que cada tabla tenga las filas esperadas y que las tres sumen la misma cantidad de URLs; si no, la tarea falla y queda en rojo en Airflow. Cada tabla tiene **clave primaria** en su grano, así que una fila duplicada ni siquiera se puede insertar.

No usamos `to_sql(if_exists="replace")` de pandas, porque hace `DROP TABLE` y se pierde la clave primaria, los comentarios de las columnas y cualquier vista que dependa de la tabla.

**Por qué mover la gold del lakehouse al warehouse:** DuckDB corre dentro de un proceso y lee archivos del lake. Sirve para transformar, pero no atiende a varios usuarios a la vez ni maneja permisos. PostgreSQL sí: Metabase se conecta con el usuario del grupo, solo ve el esquema `grupo07` y lee tablas chicas ya agregadas, que responden rápido. El tablero nunca lee del lake.

**Documentación dentro del warehouse:** `publicar_gold` copia las descripciones de `schema.yml` como `COMMENT` de PostgreSQL. Metabase las muestra al lado de cada columna, y la documentación se escribe una sola vez, en dbt.

NB06 verifica lo publicado: que las tres tablas sumen lo mismo y tengan clave primaria, las familias del último mes y los comentarios.

## 2. Tablero

**"Amenazas URLhaus — grupo07"** en Metabase. Tiene 2 indicadores y 5 gráficos, y cada uno responde una pregunta. El detalle, las consultas y la captura están en [`docs/TABLERO.md`](../TABLERO.md).

| Gráfico | Pregunta |
| --- | --- |
| Indicador | ¿Cuántas URLs maliciosas nuevas se registraron el último mes completo, y cómo viene contra el anterior? |
| Indicador | ¿Cuántas familias distintas estuvieron activas el último mes? |
| Línea | ¿Cómo evolucionó la cantidad de URLs nuevas por mes? |
| Barras apiladas | ¿Qué familias explican el volumen de cada mes? |
| Barras horizontales | ¿Qué familias sumaron y perdieron más URLs respecto del mes anterior? |
| Barras al 100 % | ¿Con qué tipo de archivo se distribuye cada familia? |

**Qué decisión se toma con esto:** un equipo de seguridad ve si el volumen sube, qué familias lo explican y si son campañas puntuales o actividad sostenida, y qué formatos bloquear o inspeccionar para cada familia. Por ejemplo, `elf` apunta a dispositivos Linux e IoT, y `exe` o `zip`, a usuarios de Windows.

**Cómo leer octubre de 2026:** el indicador marca +707 % contra septiembre. Es un pico de reportes: un solo reportero, `wristhulk`, cargó el 51 % del dataset desde el 29/09, y en ese período aparecen en masa las familias que muestra el gráfico de volumen por familia (SmartLoader, SalatStealer, zapret, LuaJIT-loader). Con estos datos no se puede separar cuánto es más malware y cuánto es más actividad de reporte.

**Carga rápido** porque cada gráfico lee tablas de unas pocas miles de filas, ya agregadas por dbt. Metabase no agrega casi nada.

## 3. Features

**Están definidos una sola vez, en el modelo dbt `fct_features_familias`.** Está versionado en el repositorio, tiene tests, lo publica el DAG y Metabase lo lee de PostgreSQL. No hay otra copia de la fórmula en ningún notebook ni en el tablero.

| Feature | Qué es |
| --- | --- |
| `cantidad_mes_anterior` | URLs de la familia en el mes calendario anterior |
| `variacion_mensual_pct` | Cambio % contra el mes anterior. Queda vacío si el anterior tuvo 0, porque no hay porcentaje posible sobre 0 |
| `promedio_movil_3m` | Promedio del mes y los dos anteriores |
| `acumulado` | Total desde la primera aparición de la familia |
| `pct_del_mes` | Participación de la familia en las URLs del mes |
| `mes_completo` | Falso para el mes en curso, cuyos valores van a cambiar |

**Los meses sin actividad se completan con 0.** Por eso "el mes anterior" y "los últimos 3 meses" son siempre meses calendario, aunque la familia haya desaparecido un tiempo. Esto importa porque muchas familias llegan por campañas: LuaJIT-loader tuvo 4.623 URLs en junio, ninguna entre julio y septiembre, y 13.762 en octubre.

**Sin fuga temporal.** Una fuga es usar, para calcular el feature de un mes, información que en ese mes todavía no existía. Por ejemplo, el promedio del año entero calculado para marzo. Acá todas las ventanas miran solo hacia atrás (`lag`, `rows between 2 preceding and current row`, `unbounded preceding`): el feature de marzo usa marzo, febrero y enero, nunca abril. El test **`assert_features_sin_fuga`** recalcula cada feature desde cero, filtrando explícitamente los datos hasta el mes del feature, y falla si no coincide.

**`pct_del_mes` y el mes en curso.** La participación usa el total del mismo mes, que se conoce recién cuando el mes termina. Para un mes cerrado no es fuga: es información que existe al cerrar el mes. Para el mes en curso, el valor cambia día a día. Por eso está la columna `mes_completo`, y los gráficos usan solo meses completos.

**Reproducibles:** es SQL versionado que se recalcula entero desde la gold en cada corrida. Con los mismos datos, da lo mismo.

**Otros tests:** `assert_features_consistentes` verifica que no haya mes y familia repetidos, que no haya valores negativos y que la participación esté entre 0 % y 100 %. En producción detectó los meses sin URLs donde la participación daba 0/0 = NaN. Además, el cuadre general verifica que los features sumen lo mismo que silver.

## 4. Governance

[`docs/GOVERNANCE.md`](../GOVERNANCE.md) tiene las seis secciones del taller:
- identificación, con responsable con nombre,
- origen y linaje, con el diagrama generado de los `ref()` de los modelos dbt,
- diccionario en lenguaje de negocio, con ejemplos y con los campos que no terminamos de entender,
- clasificación de sensibilidad por campo, con fundamento,
- calidad: verificaciones, frescura, huecos conocidos y lo que el dataset no permite responder,
- licencia según los términos vigentes de abuse.ch.
