select a."name" 
    , count(*) as count
from {{source('stg', 'work') }} w
left join stg.work_author wa 
    on w.id = wa.work_id 
left join stg.author a 
    on wa.author_id  = a.id
where a."name" is not null
group by a.name
order by count(*) desc