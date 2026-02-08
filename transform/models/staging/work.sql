select str_split((column4) ->> 'key','/')[-1] as id,
    (column4) ->> 'title' as title,
    (column4) ->> 'description' ->> 'value' as description,
    str_split((value) ->> 'author' ->> 'key', '/')[-1] as author,
    CAST((column4) ->> 'created' ->> 'value' AS TIMESTAMP) as created,
    CAST((column4) ->> 'last_modified' ->> 'value' AS TIMESTAMP) as last_modified
from {{source('books', 'work') }}, json_each(json_extract(column4, '$.authors'))
