-- Features por familia de malware y mes: variables derivadas que un modelo podría usar para
-- anticipar qué familias van a crecer. Definidas UNA sola vez, acá; el DAG las publica en
-- PostgreSQL y Metabase las lee de ahí.
--
-- Grano: una fila por (mes, familia), desde el primer mes en que aparece la familia hasta el
-- último mes con datos. Los meses sin URLs de esa familia se completan con 0: así "el mes
-- anterior" y "los últimos 3 meses" son siempre meses calendario, aunque la familia no haya
-- tenido actividad.
--
-- Sin fuga temporal: todas las ventanas miran solo hacia atrás (lag, "rows between 2 preceding
-- and current row", "unbounded preceding"). El feature de un mes nunca usa meses posteriores.
-- Lo verifica el test assert_features_sin_fuga.

{{ config(materialized='table') }}

with mensual as (
    select fecha, familia_malware, sum(cantidad_amenazas) as cantidad
    from {{ ref('fct_familias_mensual') }}
    group by 1, 2
),
limites as (
    select min(fecha) as primero, max(fecha) as ultimo from mensual
),
meses as (
    select cast(m as date) as fecha
    from limites, range(primero, ultimo + interval 1 month, interval 1 month) as t(m)
),
familias as (
    select familia_malware, min(fecha) as desde from mensual group by 1
),
grilla as (
    select m.fecha, f.familia_malware
    from familias f
    join meses m on m.fecha >= f.desde
),
base as (
    select g.fecha, g.familia_malware, coalesce(m.cantidad, 0) as cantidad_amenazas
    from grilla g
    left join mensual m using (fecha, familia_malware)
),
con_ventanas as (
    select
        b.*,
        coalesce(lag(cantidad_amenazas) over w, 0)                              as cantidad_mes_anterior,
        avg(cantidad_amenazas) over (w rows between 2 preceding and current row) as promedio_movil_3m,
        sum(cantidad_amenazas) over (w rows between unbounded preceding and current row) as acumulado,
        sum(cantidad_amenazas) over (partition by fecha)                        as total_mes
    from base b
    window w as (partition by familia_malware order by fecha)
)
select
    fecha,
    cast(extract(year from fecha) as integer)   as anio,
    cast(extract(month from fecha) as integer)  as mes,
    familia_malware,
    cast(cantidad_amenazas as bigint)           as cantidad_amenazas,
    cast(cantidad_mes_anterior as bigint)       as cantidad_mes_anterior,
    case when cantidad_mes_anterior > 0
         then round(100.0 * (cantidad_amenazas - cantidad_mes_anterior) / cantidad_mes_anterior, 2)
    end                                         as variacion_mensual_pct,
    round(promedio_movil_3m, 2)                 as promedio_movil_3m,
    cast(acumulado as bigint)                   as acumulado,
    round(100.0 * cantidad_amenazas / total_mes, 2) as pct_del_mes,
    fecha < (select ultimo from limites)        as mes_completo
from con_ventanas
