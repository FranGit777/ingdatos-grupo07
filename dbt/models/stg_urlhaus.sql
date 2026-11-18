-- Capa silver: tipado, limpieza y deduplicado de amenazas.
-- Reglas: descartar filas sin id o sin fecha; una sola observación por ID.
-- De los tags de URLhaus se sacan la familia de malware y el tipo de archivo
-- (el campo threat casi siempre es 'malware_download', así que no alcanza para distinguir amenazas).
{{ config(materialized='table') }}

{#- Tags que indican el tipo de archivo que descarga la URL -#}
{%- set tipos_archivo = ['elf', 'exe', 'dll', 'sh', 'bash', 'bat', 'cmd', 'ps1', 'powershell',
                         'js', 'vbs', 'hta', 'lnk', 'msi', 'iso', 'img', 'zip', 'rar', '7z', 'gz',
                         'tar', 'doc', 'docx', 'xls', 'xlsx', 'pdf', 'apk', 'jar', 'py', 'php',
                         'html', 'txt', 'ascii'] -%}
{#- Tags genéricos que no son una familia: arquitectura, origen o comportamiento -#}
{%- set genericos = ['32-bit', '64-bit', 'arm', 'arm4', 'arm5', 'arm6', 'arm7', 'aarch64', 'mips',
                     'mipsel', 'mpsl', 'm68k', 'ppc', 'powerpc', 'sparc', 'sh4', 'x86', 'x86-64',
                     'x86_64', 'i586', 'i686', 'amd64', 'arc', 'opendir', 'encrypted', 'geofenced',
                     'botnetdomain', 'dropper', 'downloader', 'loader', 'payload', 'malware',
                     'malicious', 'script', 'stealer', 'rat', 'trojan', 'hfs', 'censys',
                     'unknown', 'none', 'null', 'nan',
                     'malware-download', 'c2-monitor-auto', 'dll-sideloading',
                     'cowrie', 'honeypot', 'ssh', 'telnet', 'encoded',
                     'botnet', 'iot', 'automated', 'java', 'docker', 'stego',
                     'macos', 'vscode', 'nodejs', 'windows', 'linux', 'android'] -%}
{#- Tags que son una IP (con puntos o guiones, a veces con puerto: 176-65-134-121-8080) o un
    dominio (pw-dvx.to, o con guiones en lugar de puntos: bauble-cleverpondky-com). Indican de
    dónde se descargaba el archivo, no qué malware es. -#}
{%- set patron_ip = '^[0-9]{1,3}([.-][0-9]{1,3}){3}([.:-][0-9]{1,5})?$' -%}
{%- set patron_dominio = '[.-](com|net|org|info|biz|xyz|top|site|online|space|shop|store|live|club|pw|to|cc|me|io|su|ru|cn|in|br|de|uk|us)$' -%}
{#- Tags de dónde estaba alojado el archivo (servicio o plataforma), tampoco son una familia -#}
{%- set hosting = ['github', 'gitlab', 'bitbucket', 'discord', 'dropbox', 'google-drive',
                   'googledrive', 'gdrive', 'onedrive', 'mediafire', 'mega', 'pastebin',
                   'telegram', 'cdn', 'wordpress', 'aws', 'azure', 'cloudflare', 'digitalocean',
                   'ngrok', 'ipfs', 'ftp', 'http', 'https', 'sourceforge', 'firebase'] -%}

with base as (
    select
        cast(id as integer) as id_amenaza,
        cast(date_added as timestamp) as fecha_agregado,
        cast(url as varchar) as url_maliciosa,
        cast(url_status as varchar) as estado,
        cast(threat as varchar) as tipo_amenaza,
        cast(reporter as varchar) as reportero,
        -- en bronze los tags son una lista (['Mozi', 'elf']) o texto: los pasamos a texto
        cast(tags as varchar) as etiquetas
    -- DuckDB lee directamente los archivos Parquet de tu Delta Lake en MinIO
    from read_parquet('s3://{{ env_var("MINIO_BUCKET", "grupo07") }}/bronze/series/*.parquet')
    where id is not null
      and date_added is not null
),
dedup as (
    select *,
        row_number() over (
            partition by id_amenaza
            order by fecha_agregado desc
        ) as rn
    from base
),
unicos as (
    select * from dedup where rn = 1
),
-- Una fila por tag, con su posición (el orden en que URLhaus los informa)
lista as (
    select id_amenaza,
           string_split(regexp_replace(coalesce(etiquetas, ''), '[\[\]''"]', '', 'g'), ',') as tags
    from unicos
),
tags as (
    select id_amenaza,
           trim(unnest(tags)) as tag,
           generate_subscripts(tags, 1) as pos
    from lista
),
clasificados as (
    select id_amenaza, pos,
        case when lower(tag) in ('{{ tipos_archivo | join("', '") }}') then lower(tag) end as archivo,
        case when tag = ''
               or lower(tag) in ('{{ tipos_archivo | join("', '") }}')
               or lower(tag) in ('{{ genericos | join("', '") }}')
               or lower(tag) in ('{{ hosting | join("', '") }}')
               or regexp_matches(tag, '^[A-Z]{2,3}$')        -- códigos de país: CHN, USA, ...
               or regexp_matches(tag, '{{ patron_ip }}')
               or regexp_matches(lower(tag), '{{ patron_dominio }}')
               or regexp_matches(lower(tag), '^[0-9a-f]{6,}$')   -- pedazos de hash (d52f85)
               or lower(tag) like 'dropped-by%'
               or lower(tag) like 'ua-%'
             then null else tag end as familia
    from tags
),
por_id as (
    -- el primer tag que es familia y el primero que es tipo de archivo
    select id_amenaza,
           arg_min(familia, pos) filter (where familia is not null) as familia_malware,
           arg_min(archivo, pos) filter (where archivo is not null) as tipo_archivo
    from clasificados
    group by id_amenaza
),
-- Mismo nombre con distintas mayúsculas (mirai / Mirai) = una sola familia:
-- se usa la forma más frecuente de cada nombre.
grafias as (
    select familia_malware,
           row_number() over (partition by lower(familia_malware)
                              order by count(*) desc, familia_malware) as orden
    from por_id
    where familia_malware is not null
    group by familia_malware
)
select
    u.id_amenaza,
    u.fecha_agregado,
    u.url_maliciosa,
    u.estado,
    u.tipo_amenaza,
    coalesce(g.familia_malware, 'sin_familia') as familia_malware,
    coalesce(p.tipo_archivo, 'desconocido') as tipo_archivo,
    u.etiquetas,
    u.reportero
from unicos u
left join por_id p using (id_amenaza)
left join grafias g
       on lower(g.familia_malware) = lower(p.familia_malware)
      and g.orden = 1
