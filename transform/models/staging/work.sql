select str_split((data) ->> 'key','/')[-1] as id,
    (data) ->> 'title' as title,
    case when json_extract(data, '$.description.value') is not null
        then data ->> 'description' ->> 'value'
        else data ->> 'description' end as description,
    CAST((data) ->> 'created' ->> 'value' AS TIMESTAMP) as created,
    CAST((data) ->> 'last_modified' ->> 'value' AS TIMESTAMP) as last_modified
from {{source('raw', 'work') }}
where (data) ->> 'title' is not null
