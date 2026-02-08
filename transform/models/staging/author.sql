{{ config(schema='stg') }}

select str_split((column4) ->> 'key','/')[-1] as id,
    (column4) ->> 'birth_date' as birth_date,
    (column4) ->> 'name' as name,
    (column4) ->> 'title' as title,
    case when json_extract(column4, '$.bio.value') is not null then (column4) ->> 'bio' ->> 'value' else (column4) ->> 'bio' end  as bio,
    CAST((column4) ->> 'created' ->> 'value' AS TIMESTAMP) as created,
    CAST((column4) ->> 'last_modified' ->> 'value' AS TIMESTAMP) as last_modified
from {{source('books', 'author') }}
