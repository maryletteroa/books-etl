import os

import duckdb

db = "../data/books.duckdb"

if os.path.exists(db):
    os.remove(db)

# get raw data from Open Library Archive
with duckdb.connect(db) as con:

    con.execute("set memory_limit='4GB'")

    # print("downloading author")
    con.execute(
        """
        CREATE OR REPLACE TABLE raw_author AS
        SELECT *
        FROM read_csv_auto('../data/archive/ol_dump_authors_2025-12-31.txt');
        """
    )
    print("downloading ratings")
    con.execute(
        """
        CREATE OR REPLACE TABLE raw_rating AS
        SELECT *
        FROM read_csv_auto('../data/archive/ol_dump_ratings_2025-12-31.txt');
        """
    )
    print("downloading works")
    con.execute(
        """
        CREATE OR REPLACE TABLE raw_work AS
        SELECT *
        FROM read_csv_auto('../data/archive/ol_dump_works_2025-12-31.txt')
        USING SAMPLE 0.01;
        """
    )


# create or replace table author as
# select str_split((column4) ->> 'key','/')[-1] as id,
# (column4) ->> 'birth_date' as birth_date,
# (column4) ->> 'name' as name,
# (column4) ->> 'title' as title,
# case when json_extract(column4, '$.bio.value') is not null then (column4) ->> 'bio' ->> 'value' else (column4) ->> 'bio' end  as bio,
# CAST((column4) ->> 'created' ->> 'value' AS TIMESTAMP) as created,
# CAST((column4) ->> 'last_modified' ->> 'value' AS TIMESTAMP) as last_modified
# from raw_author;
