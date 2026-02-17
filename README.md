# About 
An ETL project using books data from the [Open Library API](https://openlibrary.org/developers/api).

This is a simple implementation of a local modern data stack using the following tools:
- **Duckdb** as database
- **dbt** as transformation and data quality tool
- **Dagster** as scheduler
- **Streamlit** as visualizer


# Run
Use the Makefile to run the commands. Dependencies uses `uv`

1. Install [`uv`](https://github.com/astral-sh/uv) Python package manager.
2. Install dependencies, and create a virtual environment:
```sh
make install
```
3. Download the data. Work IDs in this sample are based off the ratings dump as of December 2025, provided here. Newer rating file can be downloaded from the Open Library [data dumps](https://openlibrary.org/developers/dumps).
```sh
make download nsample=<number of rows to sample> random=<true/false>
```
4. Transform data and check quality. As Dagster can materialize the tables using the dependency tree from dbt, this step is optional. Instead, run the job from Dagster to materialize the tables
```sh
make dbt
```
5. Serve job scheduler
```sh
make dagster
```
5. Serve data visualization
```sh
make streamlit
```