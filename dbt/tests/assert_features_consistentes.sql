-- Reglas que tiene que cumplir cualquier fila de features:
--   * una sola fila por (mes, familia),
--   * cantidades y acumulados no negativos, y el acumulado nunca menor que el mes,
--   * participación en el mes entre 0 % y 100 %,
--   * solo el último mes puede figurar como incompleto.
-- El test falla si esta consulta devuelve alguna fila.
with duplicados as (
    select fecha, familia_malware, 'grano duplicado' as problema
    from {{ ref('fct_features_familias') }}
    group by 1, 2
    having count(*) > 1
),
fuera_de_rango as (
    select fecha, familia_malware, 'valor fuera de rango' as problema
    from {{ ref('fct_features_familias') }}
    where cantidad_amenazas < 0
       or cantidad_mes_anterior < 0
       or acumulado < cantidad_amenazas
       or pct_del_mes < 0 or pct_del_mes > 100
),
incompletos as (
    select fecha, familia_malware, 'mes incompleto que no es el último' as problema
    from {{ ref('fct_features_familias') }}
    where not mes_completo
      and fecha < (select max(fecha) from {{ ref('fct_features_familias') }})
)
select * from duplicados
union all select * from fuera_de_rango
union all select * from incompletos
