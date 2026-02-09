/*select str_split((data),'/')[-1] as work_id,
    str_split((column1),'/')[-1] as book_id,
    column2 as rating,
    column3 as date
from {{source('books', 'rating') }}
*/


select data ->> 'summary' ->> 'average' as average,
	data ->> 'summary' ->> 'count' as count,
	data ->> 'counts' ->> '1' as rating_1,
	data ->> 'counts' ->> '2' as rating_2,
	data ->> 'counts' ->> '3' as rating_3,
	data ->> 'counts' ->> '4' as rating_4,
	data ->> 'counts' ->> '5' as rating_5,
from {{source('books', 'rating') }}
