with cte as (
    select str_split((data) ->> 'key','/')[-1] as work_id,
        (data) ->> 'subject_places' as subject_place,
    from {{source('raw', 'work') }}
)

select 
    work_id,
	unnest(list_transform(
        string_split(trim(subject_place, '[]'), '","'),
        x -> trim(x, '"')
    )) as subject_place
from cte