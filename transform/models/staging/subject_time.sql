with cte as (
    select str_split((data) ->> 'key','/')[-1] as work_id,
        (data) ->> 'subject_times' as subject_time,
    from {{source('raw', 'work') }}
)

select 
    work_id,
	unnest(list_transform(
        string_split(trim(subject_time, '[]'), '","'),
        x -> trim(x, '"')
    )) as subject_time
from cte