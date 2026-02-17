import duckdb
import streamlit as st

st.set_page_config(page_title="Books", layout="wide")
st.title("Books")

st.header("Top Subjects")
with duckdb.connect("../../data/books.duckdb") as conn:

    # Query returns a DuckDB relation
    result = conn.execute(
        """
                        SELECT subject, count FROM mart.ct_subject
                        order by count desc
                        limit 10
                        """
    ).df()

    st.bar_chart(
        result,
        x="subject",
        y="count",
        sort="-count",
        horizontal=True,
        width=1500,
        height=500,
    )
