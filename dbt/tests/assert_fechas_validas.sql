-- Regla de dominio: URLhaus empezó a registrar URLs en 2018 y no puede haber URLs registradas
-- en el futuro. Una fecha fuera de ese rango indica un error de parseo o de zona horaria en la
-- ingesta (por ejemplo, una fecha local tomada como UTC o un campo mal leído del CSV).
-- El test falla si esta consulta devuelve alguna fila.
select id_amenaza, fecha_agregado
from {{ ref('stg_urlhaus') }}
where fecha_agregado < timestamp '2018-01-01'
   or fecha_agregado > cast(now() as timestamp) + interval 1 day
