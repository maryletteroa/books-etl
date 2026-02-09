with cte as (
    select str_split((data) ->> 'key','/')[-1] as work_id,
        (data) ->> 'subjects' as subject,
    from {{source('raw', 'work') }}
)

select 
    work_id,
	unnest(list_transform(
        string_split(trim(subject, '[]'), '","'),
        x -> trim(x, '"')
    )) as subject
from cte