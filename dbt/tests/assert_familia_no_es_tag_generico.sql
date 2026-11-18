-- Regla de dominio: una familia de malware nunca puede ser un tag genérico de URLhaus
-- (arquitectura, tipo de archivo, hosting o país). Si aparece uno, la lista de tags genéricos
-- de stg_urlhaus quedó desactualizada y el ranking de familias del tablero estaría mal.
-- El test falla si esta consulta devuelve alguna fila.
select familia_malware, count(*) as urls
from {{ ref('stg_urlhaus') }}
where lower(familia_malware) in ('elf', 'exe', 'zip', 'sh', '32-bit', '64-bit', 'arm', 'mips',
                                 'x86', 'github', 'discord', 'dropbox', 'opendir', 'ua-wget')
   or regexp_matches(familia_malware, '^[A-Z]{2,3}$')
group by familia_malware
