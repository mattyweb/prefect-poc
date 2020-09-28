# Prefect Data loading POC

This proof-of-concept represents a new, standalone version of a data ingestion pipeline that load data downloaded from Socrata into the 311 Data project Postgres database.

## Installation/Running

```bash
# download the project
# from the project root
pip install requirements.txt
# add socrata token and dsn to config.yaml
python flow.py
```

## Socrata

The data comes from the Los Angeles 311 system as exposed though a [Socrata](https://dev.socrata.com/) instance [hosted by the city](https://data.lacity.com).

Socrata has a python library called [Sodapy](https://github.com/xmunoz/sodapy) which acts as a client for its API and is used in this project.

## Prefect

The engine managing the data ingestion process is [Prefect Core](https://www.prefect.io/core). [Prefect](https://www.prefect.io/) is a company built around an open source library in a so-called [open-core model](https://en.wikipedia.org/wiki/Open-core_model).

In the "Prefect" idiom, there are [tasks](https://docs.prefect.io/core/concepts/tasks.html) which perform an action and are chained together into a [flow](https://docs.prefect.io/core/concepts/flows.html). Flows can be executed in a number of different ways, but this project is set up to run the flow as a python file via the command line:

```bash
python flow.py
```

The steps in the flow:

* Configure and start the flow
* Download data from Socrata
* Insert the downloaded data to a temporary Postgres table
* Move data from the temporary to requests table
* Update views
* Write some metadata about the load

## Configuration

Configuration is done via a YAML file called config.yaml in the project root.

### Secrets

The Socrata token and DSN secrets are expected to be provided as environment variables.

```bash
SOCRATA_TOKEN=H3lljlkl43232jl
DSN=postgresql://user_name:user_pass@localhost:5432/db_name
```

### Flow configuration

* domain: the path to the Socrata instance (e.g. "data.lacity.org")
* dask: setting this to True will run the download steps in parallel
* datasets: a dictionary of years and Socrata dataset keys

### Data configuration

* years: the years to be loaded
* since: will only load records change since this date (note that if since is specified it will load updated data for ALL years)
* fields: a dictionary of fields and their Postgres data type (note that this will assume varchar unless specified otherwise)
* the key to be used to manage inserts/updates (e.g. "srnumber")

## To-dos/Next steps

* Allow all data fields/attributes to be configured in YAML
* Parallelize the loading step
* Remove the need to store/load from CSV files
