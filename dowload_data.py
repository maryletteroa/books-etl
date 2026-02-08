import os

import duckdb

db = "../data/books.duckdb"

if os.path.exists(db):
    os.remove(db)


def get_data(full: bool = False):
    with duckdb.connect(db) as con:

        con.execute("set memory_limit='4GB'")
        con.execute("create schema if not exists raw")

        for record in ["author", "rating", "work"]:
            print(f"downloading {record}")
            if full:
                source_path = f"../data/dump/ol_dump_{record}_2025-12-31.txt"
            else:
                source_path = f"../data/sample/{record}.tsv"
            con.execute(
                f"""
                CREATE OR REPLACE TABLE raw.{record} AS
                SELECT *
                FROM read_csv_auto('{source_path}');
                """
            )


def get_sample_data():
    # random works id
    # get author information
    # create keys
    #
    pass


if __name__ == "__main__":
    get_data(full=False)
