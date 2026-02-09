select str_split((data) ->> 'key','/')[-1] as work_id,
    str_split((value) ->> 'author' ->> 'key', '/')[-1] as author_id,
from {{source('raw', 'work') }}, json_each(json_extract(data, '$.authors'))
