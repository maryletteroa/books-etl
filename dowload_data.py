import asyncio
import json
import os
import time
from typing import Literal

import aiohttp
import duckdb


async def fetch(session, url):
    async with session.get(url) as response:
        return url, await response.json()


async def load_data(
    con: duckdb.connect, endpt: Literal["work", "author", "rating"], ids: list
):
    base_url = {
        "work": "https://openlibrary.org/works/id.json",
        "author": "https://openlibrary.org/authors/id.json",
        "rating": "https://openlibrary.org/works/id/ratings.json",
    }
    urls = [base_url[endpt].replace("id", id) for id in ids]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

    for url, result in results:
        status = result.get("status")
        message = result.get("message")
        if status:
            raise Exception(message)
        if endpt == "rating":
            id = url.split("/")[-2]
        else:
            id = url.split("/")[-1].split(".")[0]
        con.execute(
            f"insert into raw.{endpt} values(?, json(?))", [id, json.dumps(result)]
        )


def sample_works(duckdb_path: str):
    with duckdb.connect(duckdb_path) as con:

        con.execute("set memory_limit='4GB'")
        con.execute("create schema if not exists raw")

        source_path = "../data/dump/ol_dump_work_2025-12-31.txt"
        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw.work AS
            SELECT *
            FROM read_csv_auto('{source_path}')
            using sample reservoir(500 rows)
            repeatable (42);
            """
        )


def sample_rating(duckdb_path: str):
    with duckdb.connect(duckdb_path) as con:

        con.execute("set memory_limit='4GB'")
        con.execute("create schema if not exists raw")

        source_path = "../data/dump/ol_dump_rating_2025-12-31.txt"
        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw.sample_rating AS
            SELECT *
            FROM read_csv_auto('{source_path}')
            using sample reservoir(500 rows)
            repeatable (42);
            """
        )


def get_ids(duckdb_path: str, endpt: Literal["work", "rating"]):
    with duckdb.connect(duckdb_path) as con:

        if endpt == "work":
            ids = con.execute(
                """
                    select
                        str_split((data) ->> 'key','/')[-1] as work_id,
                        str_split((value) ->> 'author' ->> 'key', '/')[-1] as author_id
                    from raw.work, json_each(json_extract(data, '$.authors'))
                """
            ).fetchall()
        elif endpt == "rating":

            ids = con.execute(
                """
                    select str_split(column0,'/')[-1] as work_id
                    , null
                    from raw.sample_rating sr 
                """
            ).fetchall()

        work_ids = [w for w in set([row[0] for row in ids])]
        author_ids = [w for w in set([row[1] for row in ids])]

        return work_ids, author_ids


def batch_load(
    con: duckdb.connect, endpt: Literal["author", "rating", "work"], ids: list
):
    print(f"downloading {endpt}...")
    con.execute("set memory_limit='4GB'")
    con.execute(f"create or replace table raw.{endpt} (id string, data json)")
    nbatch = 10
    ids_batch = [ids[i : i + nbatch] for i in range(0, len(ids), nbatch)]
    ntotal_batch = len(ids_batch)
    for i, ib in enumerate(ids_batch, start=1):
        print(f"{i}/{ntotal_batch} ({nbatch * i} rows)", end="\r")
        asyncio.run(load_data(con=con, endpt=endpt, ids=ib))


if __name__ == "__main__":

    duckdb_path = "../data/books.duckdb"
    if os.path.exists(duckdb_path):
        os.remove(duckdb_path)

    start = time.perf_counter()

    # get works data based on those with ratings
    print("sampling rating...")
    sample_rating(duckdb_path=duckdb_path)
    work_ids, _ = get_ids(duckdb_path=duckdb_path, endpt="rating")
    with duckdb.connect(duckdb_path) as con:
        batch_load(con=con, endpt="work", ids=work_ids)

    # get authors and ratings data from api
    work_ids, author_ids = get_ids(duckdb_path=duckdb_path, endpt="work")
    with duckdb.connect(duckdb_path) as con:
        batch_load(con=con, endpt="author", ids=author_ids)
        batch_load(con=con, endpt="rating", ids=work_ids)

    end = time.perf_counter()
    elapsed = end - start
    print(f"time taken: {elapsed / 60:2f} minutes, {elapsed % 60:2f} seconds.")
