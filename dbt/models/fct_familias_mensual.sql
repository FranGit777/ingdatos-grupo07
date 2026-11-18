-- Capa gold atómica: el grano más fino que publicamos para el negocio / BI.
-- Grano: cantidad de amenazas por mes, tipo de amenaza, familia de malware y tipo de archivo.
-- Cada URL cuenta una sola vez. Los demás resúmenes (fct_amenazas_mensual) salen de esta tabla.

{{ config(materialized='table') }}

select
    cast(date_trunc('month', fecha_agregado) as date) as fecha,
    extract(year from fecha_agregado) as anio,
    extract(month from fecha_agregado) as mes,
    tipo_amenaza,
    familia_malware,
    tipo_archivo,
    count(id_amenaza) as cantidad_amenazas
from {{ ref('stg_urlhaus') }}
group by 1, 2, 3, 4, 5, 6
order by fecha desc, cantidad_amenazas desc
