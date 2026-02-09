select str_split((data) ->> 'key','/')[-1] as id,
    (data) ->> 'birth_date' as birth_date,
    (data) ->> 'name' as name,
    (data) ->> 'title' as title,
    case when json_extract(data, '$.bio.value') is not null then (data) ->> 'bio' ->> 'value' else (data) ->> 'bio' end  as bio,
    CAST((data) ->> 'created' ->> 'value' AS TIMESTAMP) as created,
    CAST((data) ->> 'last_modified' ->> 'value' AS TIMESTAMP) as last_modified
from {{source('books', 'author') }}
