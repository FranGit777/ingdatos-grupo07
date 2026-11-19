-- Capa gold diaria: URLs nuevas por día. Sirve para seguir el mes en curso mientras pasa
-- (el mes que va contra el mismo tramo del mes anterior), sin esperar a que termine.
-- El día es el de registro en URLhaus, en UTC, igual que los meses de las otras tablas gold.

{{ config(materialized='table') }}

select
    cast(fecha_agregado as date) as fecha,
    count(id_amenaza) as cantidad_amenazas
from {{ ref('stg_urlhaus') }}
group by 1
order by fecha desc
