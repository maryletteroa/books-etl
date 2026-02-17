# -------------------------
# Makefile: Dagster + Streamlit
# -------------------------

.DEFAULT_GOAL := help

help:
	@echo ""
	@echo "Available commands:"
	@echo "  make install	- Install dependencies in a virtual environment"
	@echo "  make dbt	- Start dbt transform"
	@echo "  make dagster	- Start Dagster dev server"
	@echo "  make streamlit - Run Streamlit app"
	@echo ""

install:
	uv sync

data:
ifeq ($(random),true)
	source .venv/bin/activate &&  python extract --random --nsample ${nsample}
else
ifeq ($(random),false)
	source .venv/bin/activate &&  python extract --nsample ${nsample}
else
	$(error Accepted value for random: true or false only)
endif
endif


dbt:
	source .venv/bin/activate && cd transform && dbt run

dagster:
	source .venv/bin/activate && cd schedule && dagster dev

streamlit:
	source .venv/bin/activate && cd visualize && streamlit run app.py


