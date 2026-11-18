-- Fuga temporal: cada feature de un mes tiene que poder calcularse SOLO con datos de ese mes o
-- anteriores. Este test los recalcula de cero desde la gold atómica, filtrando explícitamente
-- fecha <= mes del feature, y compara. Si alguien cambia una ventana para que mire hacia
-- adelante (por ejemplo "following" o el promedio del mes completo), los valores no coinciden.
-- El test falla si esta consulta devuelve alguna fila.
with gold as (
    select fecha, familia_malware, sum(cantidad_amenazas) as cantidad
    from {{ ref('fct_familias_mensual') }}
    group by 1, 2
),
desde as (
    select familia_malware, min(fecha) as primero from gold group by 1
),
recalculado as (
    select
        f.fecha,
        f.familia_malware,
        coalesce((select sum(g.cantidad) from gold g
                  where g.familia_malware = f.familia_malware
                    and g.fecha = f.fecha - interval 1 month), 0)            as anterior,
        coalesce((select sum(g.cantidad) from gold g
                  where g.familia_malware = f.familia_malware
                    and g.fecha <= f.fecha), 0)                              as acumulado,
        coalesce((select sum(g.cantidad) from gold g
                  where g.familia_malware = f.familia_malware
                    and g.fecha <= f.fecha
                    and g.fecha >= f.fecha - interval 2 month), 0)
          / least(3, date_diff('month', d.primero, f.fecha) + 1)             as promedio
    from {{ ref('fct_features_familias') }} f
    join desde d using (familia_malware)
)
select f.fecha, f.familia_malware,
       f.cantidad_mes_anterior, r.anterior,
       f.acumulado, r.acumulado as acumulado_recalculado,
       f.promedio_movil_3m, round(r.promedio, 2) as promedio_recalculado
from {{ ref('fct_features_familias') }} f
join recalculado r using (fecha, familia_malware)
where f.cantidad_mes_anterior <> r.anterior
   or f.acumulado <> r.acumulado
   or abs(f.promedio_movil_3m - r.promedio) > 0.01
