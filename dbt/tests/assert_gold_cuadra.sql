-- Las tablas gold, la de features y la silver tienen que sumar lo mismo: cada URL cuenta una sola vez.
-- El test falla si esta consulta devuelve alguna fila.
with totales as (
    select
        (select count(*) from {{ ref('stg_urlhaus') }}) as silver,
        (select sum(cantidad_amenazas) from {{ ref('fct_amenazas_mensual') }}) as gold_tipo,
        (select sum(cantidad_amenazas) from {{ ref('fct_familias_mensual') }}) as gold_familia,
        (select sum(cantidad_amenazas) from {{ ref('fct_features_familias') }}) as features
)
select * from totales
where silver <> gold_tipo or silver <> gold_familia or silver <> features
