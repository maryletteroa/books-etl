select subject, count(*) as count
from {{source('stg', 'subject') }}
group by subject
order by count(*) desc