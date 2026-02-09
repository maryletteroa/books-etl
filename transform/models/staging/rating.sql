/*select str_split((data),'/')[-1] as work_id,
    str_split((column1),'/')[-1] as book_id,
    column2 as rating,
    column3 as date
from {{source('raw', 'rating') }}
*/


select id as work_id,
	cast(data ->> 'summary' ->> 'average' as double) as average,
	cast(data ->> 'summary' ->> 'count' as int) as count,
	cast(data ->> 'counts' ->> '1' as int) as rating_1,
	cast(data ->> 'counts' ->> '2' as int) as rating_2,
	cast(data ->> 'counts' ->> '3' as int) as rating_3,
	cast(data ->> 'counts' ->> '4' as int) as rating_4,
	cast(data ->> 'counts' ->> '5' as int) as rating_5,
from {{source('raw', 'rating') }}
