FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git libpq-dev gcc

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

# Switch back to airflow user and install the requirements
USER airflow

# If you're using BuildKit (Docker 18.09+), you can use --mount, otherwise directly copy requirements.txt
# COPY requirements.txt ./
# RUN pip install -r requirements.txt

# Install requirements and vnstock3 module
RUN --mount=type=bind,target=./requirements.txt,src=./requirements.txt \
    pip install -r requirements.txt

# Copy DAGs, include, and plugins
COPY --chown=airflow:airflow ../dags /app/dags
COPY --chown=airflow:airflow ../plugins /app/plugins
COPY --chown=airflow:airflow ../config /app/config
COPY --chown=airflow:airflow ../data /app/data

WORKDIR /app
