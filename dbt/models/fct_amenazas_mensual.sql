-- Capa gold agregada: resumen de fct_familias_mensual por mes y tipo de amenaza.
-- Se calcula desde la gold atómica (no desde la silver), así las dos no se pueden desincronizar.

{{ config(materialized='table') }}

select
    anio,
    mes,
    tipo_amenaza,
    sum(cantidad_amenazas) as cantidad_amenazas
from {{ ref('fct_familias_mensual') }}
group by 1, 2, 3
order by anio desc, mes desc, cantidad_amenazas desc
