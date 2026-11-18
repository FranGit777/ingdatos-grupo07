-- URLhaus publica date_added en UTC. Si DuckDB corre con otra zona horaria (por ejemplo la
-- de una máquina en Argentina), cast(... as timestamp) corre las fechas 3 horas y las URLs de
-- las últimas horas de cada mes se cuentan en el mes anterior. profiles.yml fija TimeZone=UTC.
-- El test falla si esta consulta devuelve alguna fila.
select current_setting('TimeZone') as zona_horaria
where current_setting('TimeZone') <> 'UTC'
