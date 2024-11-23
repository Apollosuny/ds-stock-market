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

Import file from plugins folder

```bash
  # Wrong
  from plugins.models.* import abc

  # Correct
  from models.* import abc
```
  <!-- Login airflow -->
  user: airflow
  password: airflow
