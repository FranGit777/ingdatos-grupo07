# Tablero "Amenazas URLhaus — grupo07"

En **https://metabase.nexoacademia.org**, base `warehouse`, esquema `grupo07`. El tablero no se versiona en Metabase: este archivo y la captura son lo que viaja con el repo, para poder rehacerlo igual en la defensa.

![Tablero](tablero.png)

*Captura del 19/11/2026.*

## Cómo está ordenado

El tablero se lee de arriba hacia abajo como una historia en tres partes, y cada parte responde la pregunta que deja la anterior:

1. **Este mes** (filas 1 y 2): si el tablero está al día, cuántas URLs y familias van en el mes en curso, cuáles circulan más y a qué ritmo llegan por día. Es lo que cambia apenas entra un dato nuevo.
2. **El mes pasado** (fila 3): cómo cerró el último mes completo contra el anterior, en la serie de los últimos meses.
3. **Quiénes y cómo** (fila 4): qué familias explican el volumen de cada mes y en qué formato se distribuyen, que es lo que define qué bloquear.

Los títulos dicen el período de cada tarjeta: "este mes" es el mes en curso, incompleto; "último mes completo" es el mes cerrado. Las tarjetas del mes en curso no se comparan con meses cerrados, porque un mes a medias siempre parecería bajar.

## Cómo leer los números de octubre de 2026

El indicador del último mes completo marca +707 % contra septiembre y la evolución mensual sube de golpe. **Es sobre todo un pico de reportes:** octubre tuvo casi 4.000 URLs por día contra unas 500 en agosto y septiembre. El gráfico de URLs por día muestra que el pico se concentró en pocos días: entre el 24 y el 27 de octubre entraron entre 13.000 y 23.000 URLs diarias, y en noviembre el ritmo vuelve a unas 1.000. Coincide con un solo reportero, `wristhulk`, que desde el 29/09/2026 cargó 96.956 URLs (el 51 % del dataset). En el mismo período aparecen en masa las familias del volumen mensual (SmartLoader, SalatStealer, zapret, LuaJIT-loader). Con estos datos no se puede separar cuánto es más malware y cuánto es más actividad de reporte.

Lo que sí es cobertura: antes de agosto de 2026 los meses tienen pocas URLs porque el export de URLhaus ya no las conserva (ver `INCIDENTE_2026-11-18_backfill.md`). La evolución mensual sirve para comparar los meses recientes, no para medir tendencias de largo plazo.

## Tarjetas

Se arman con *Nuevo → Consulta SQL*, eligiendo la base **warehouse** (no "Sample Database"), salvo la 6, que se arma con el editor visual. Los números siguen el orden de lectura del tablero.

### Fila 1 · Este mes

**1 · Días sin actualizar** — *Número*. Días desde la última publicación exitosa del DAG. Lo normal es 0, porque el DAG publica todas las noches. Si muestra 1 o más, el tablero tiene datos viejos: el control de volumen frenó la publicación o el DAG no corrió. Formato condicional: verde en 0, rojo desde 1.
```sql
select current_date - max(publicado_en at time zone 'America/Argentina/Cordoba')::date as dias_sin_actualizar
from grupo07.estado_publicacion
where resultado = 'publicada'
```
Para ver qué pasó (consulta aparte, no va en el tablero):
```sql
select publicado_en, resultado, detalle, urls_publicadas
from grupo07.estado_publicacion
order by publicado_en desc
limit 10
```

**2 · URLs nuevas este mes** — *Número*. Sube con cada corrida del DAG: es lo primero que cambia cuando entra un dato nuevo.
```sql
select sum(cantidad_amenazas) as urls_en_lo_que_va_del_mes
from grupo07.fct_amenazas_diarias
where fecha >= (select date_trunc('month', max(fecha))::date from grupo07.fct_amenazas_diarias)
```

**3 · Familias activas este mes** — *Número*. Familias distintas con al menos una URL en el mes en curso, sin contar `sin_familia`. Mide la diversidad de la amenaza: el volumen puede subir por una sola familia o por muchas a la vez.
```sql
select count(distinct familia_malware) as familias_activas
from grupo07.fct_familias_mensual
where fecha = (select max(fecha) from grupo07.fct_familias_mensual)
  and familia_malware <> 'sin_familia'
  and cantidad_amenazas > 0
```

**4 · Top 10 familias más activas este mes** — *Fila* (barras horizontales). Qué está circulando ahora, antes de que el mes cierre.
```sql
select familia_malware, sum(cantidad_amenazas) as urls
from grupo07.fct_familias_mensual
where fecha = (select max(fecha) from grupo07.fct_familias_mensual)
  and familia_malware <> 'sin_familia'
group by 1
order by 2 desc
limit 10
```

### Fila 2 · El ritmo

**5 · URLs nuevas por día, últimos 30 días** — *Barras* (eje X: día; eje Y: URLs nuevas). Muestra el ritmo diario de reportes y los días con picos, que en los gráficos mensuales no se ven. Une las dos partes del tablero: los 30 días cruzan del mes pasado al actual.
```sql
select fecha, cantidad_amenazas as urls_nuevas
from grupo07.fct_amenazas_diarias
where fecha > (select max(fecha) from grupo07.fct_amenazas_diarias) - 30
order by fecha
```

### Fila 3 · El mes pasado

**6 · URLs nuevas del último mes completo** — *Tendencia*, con la comparación *Mes anterior*. Se arma con el editor visual (*Nuevo → Pregunta*), no con SQL, para que Metabase sepa que agrupa por mes y muestre "octubre 2026" en lugar de "octubre 1, 2026": tabla **Fct Features Familias**, filtro *Mes Completo* es verdadero, *Suma de Cantidad Amenazas* agrupada por *Fecha: Mes*. Equivale a:
```sql
select fecha as mes, sum(cantidad_amenazas) as urls_nuevas
from grupo07.fct_features_familias
where mes_completo
group by 1 order by 1
```

**7 · Evolución mensual** — *Línea*, solo meses completos de los últimos 18 meses. Al lado del 6, muestra el salto que mide su porcentaje. Antes de agosto de 2026 el histórico está incompleto (ver arriba).
```sql
select fecha as mes, sum(cantidad_amenazas) as urls_nuevas
from grupo07.fct_features_familias
where mes_completo
  and fecha >= (select max(fecha) from grupo07.fct_features_familias) - interval '18 months'
group by 1
order by 1
```

### Fila 4 · Quiénes y cómo

**8 · ¿Con qué tipo de archivo se distribuye cada familia?** — *Barras apiladas al 100 %*, una barra por familia (las 8 más activas de los últimos 6 meses). Es la decisión del tablero: qué formato bloquear o inspeccionar para cada familia.
```sql
with desde as (select max(fecha) - interval '6 months' as f from grupo07.fct_familias_mensual),
top as (
    select fm.familia_malware from grupo07.fct_familias_mensual fm, desde
    where fm.familia_malware <> 'sin_familia' and fm.fecha > desde.f
    group by 1 order by sum(fm.cantidad_amenazas) desc limit 8
)
select f.familia_malware as familia,
       case when f.tipo_archivo = 'desconocido' then 'sin dato' else f.tipo_archivo end as tipo_de_archivo,
       sum(f.cantidad_amenazas) as urls
from grupo07.fct_familias_mensual f
join top using (familia_malware), desde
where f.fecha > desde.f
group by 1, 2 order by 1, 3 desc
```

**9 · Volumen mensual por familia** — *Barras apiladas* (eje X: mes; series: familia), los últimos 6 meses completos. Muestra qué familias explican cada mes y cuáles llegan en oleadas: aparecen fuertes un mes y desaparecen.
```sql
with ult as (select max(fecha) as f from grupo07.fct_features_familias where mes_completo),
ventana as (
    select ff.* from grupo07.fct_features_familias ff, ult
    where ff.fecha > ult.f - interval '6 months' and ff.fecha <= ult.f
),
top as (
    select familia_malware from ventana where familia_malware <> 'sin_familia'
    group by 1 order by sum(cantidad_amenazas) desc limit 6
)
select v.fecha as mes,
       case when v.familia_malware in (select familia_malware from top) then v.familia_malware
            when v.familia_malware = 'sin_familia' then 'sin familia identificada'
            else 'otras familias' end as familia,
       sum(v.cantidad_amenazas) as urls
from ventana v
group by 1, 2
having sum(v.cantidad_amenazas) > 0
order by 1, 3 desc
```

## Armado

| Fila | Tarjetas |
| --- | --- |
| 1 · Este mes | 1 Días sin actualizar · 2 URLs nuevas este mes · 3 Familias activas este mes · 4 Top 10 familias (a la derecha, ocupa las filas 1 y 2) |
| 2 · El ritmo | 5 URLs nuevas por día, últimos 30 días |
| 3 · El mes pasado | 6 URLs nuevas del último mes completo · 7 Evolución mensual |
| 4 · Quiénes y cómo | 8 Tipo de archivo por familia · 9 Volumen mensual por familia |

## Qué decisión se toma con esto

Un equipo de seguridad ve si el tablero está al día (1), qué está entrando ahora y a qué ritmo (2 a 5), cómo cerró el mes pasado y si el volumen sube (6 y 7), qué familias lo explican y si son oleadas o actividad sostenida (9), y qué formatos bloquear o inspeccionar para cada una (8: `elf` apunta a dispositivos Linux e IoT; `exe` o `zip`, a usuarios de Windows).

**Para leer bien los datos:** la fecha es cuándo URLhaus *registró* la URL, no cuándo ocurrió el ataque; el mes en curso está incompleto (por eso las tarjetas 6 a 9 usan solo meses completos y las 2 a 4 muestran lo que va del mes sin compararlo); antes de agosto de 2026 el histórico está incompleto porque el origen ya no publica esas URLs; la familia es aproximada, porque sale de los tags que carga cada reportero.
