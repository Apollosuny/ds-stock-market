# Data warehouse: Amazon Sales

Small project about data warehouse

## Resources

- [Amazon Sales Report](https://www.kaggle.com/datasets/mdsazzatsardar/amazonsalesreport/data)
  Note: Install data and move it to data folder

## Installation

Create python env

```bash
  python -m venv my-env
```

Install packages

```bash
  pip install -r requirements.txt
```

Running Airflow & Database

```bash
  MacOS: docker-compose up
  Window: docker compose up
```

Access Database

```bash
  # Access container
  docker exec -it <container_id_or_name> bash

  # Access psql
  psql -U <username> -d <database_name>
```

Create User in Airflow

```bash
  airflow users create \
    --username john_doe \
    --firstname John \
    --lastname Doe \
    --role Admin \
    --email john.doe@example.com \
    --password abc
```

Data pipeline

```bash
  initial_database >> insert_data_to_db >> create_dim >> create_fact
```
