select str_split((column4) ->> 'key','/')[-1] as work_id,
    str_split((value) ->> 'author' ->> 'key', '/')[-1] as author_id,
from {{source('books', 'work') }}, json_each(json_extract(column4, '$.authors'))
