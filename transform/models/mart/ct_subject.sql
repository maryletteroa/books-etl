select subject, count(*) as count
from {{ref('subject') }}
group by subject
order by count(*) desc