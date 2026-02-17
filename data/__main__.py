import argparse
import os
import time

import duckdb
from dowload_data import batch_load, get_ids, sample_rating

parser = argparse.ArgumentParser()
parser.add_argument(
    "--random", action="store_true", help="Use random sampling for ratings"
)
args = parser.parse_args()

duckdb_path = "../data/books.duckdb"
if os.path.exists(duckdb_path):
    os.remove(duckdb_path)

start = time.perf_counter()

# get works data based on those with ratings
print("sampling rating...")
sample_rating(duckdb_path=duckdb_path, random=args.random)
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
