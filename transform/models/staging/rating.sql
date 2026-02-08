select str_split((column0),'/')[-1] as work_id,
    str_split((column1),'/')[-1] as book_id,
    column2 as rating,
    column3 as date
from {{source('books', 'rating') }}
