-- Regla de dominio: una familia de malware nunca puede ser un tag genérico de URLhaus
-- (arquitectura, tipo de archivo, hosting, país, IP o dominio). Si aparece uno, la lista de tags genéricos
-- de stg_urlhaus quedó desactualizada y el ranking de familias del tablero estaría mal.
-- El test falla si esta consulta devuelve alguna fila.
select familia_malware, count(*) as urls
from {{ ref('stg_urlhaus') }}
where lower(familia_malware) in ('elf', 'exe', 'zip', 'sh', '32-bit', '64-bit', 'arm', 'mips',
                                 'x86', 'github', 'discord', 'dropbox', 'opendir', 'ua-wget',
                                 'malware-download', 'c2-monitor-auto', 'dll-sideloading')
   or regexp_matches(familia_malware, '^[A-Z]{2,3}$')
   -- una IP (176-65-139-204, 94.154.43.26, con o sin puerto)
   or regexp_matches(familia_malware, '^[0-9]{1,3}([.-][0-9]{1,3}){3}([.:-][0-9]{1,5})?$')
   -- un dominio (pw-dvx.to, bauble-cleverpondky-com)
   or regexp_matches(lower(familia_malware), '[.-](com|net|org|xyz|top|site|space|to|ru|cn)$')
group by familia_malware
