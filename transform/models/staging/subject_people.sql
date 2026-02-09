with cte as (
    select str_split((data) ->> 'key','/')[-1] as work_id,
        (data) ->> 'subject_people' as subject_people,
    from {{source('raw', 'work') }}
)

select 
    work_id,
	unnest(list_transform(
        string_split(trim(subject_people, '[]'), '","'),
        x -> trim(x, '"')
    )) as subject_people
from cte