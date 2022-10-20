# Assessment

## Setup Airflow
By following these [instructions](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). Using Airflow 2.4.1

Or you can simply run `docker-compose up airflow-init` then `docker-compose up -d` after cloning the project and create `.env` that mentioned in the docs.

## Restore Chinook Tables and HR Tables

Chinook (PostgreSQL)
```
psql -h <hostname> -U <username> -d <database-name> -f ~/path/to/chinook_pg.sql
```

Human Resource (MySQL)
```
mysql -u <username> --host=<hostname> --port=<port> <dbname> -p < ~/path/to/human_resources_mysql.sql
```

# Task 1
## Data Modelling
![data-modelling-task-1](images/task-1.png)
![data-modelling-task-1.2](images/task-12.png)

## DAG
![dag-task-1](images/dag-sales.png)
![dag-task-1.2](images/dag-hr.png)

# Task 2
## DAG
![data-modelling-task-2](images/dag-flights.png)

## Tables
![tables-task-2](images/table-task-2.png)

# Task 3
Setup Great Expectations [here](https://docs.greatexpectations.io/docs/tutorials/getting_started/tutorial_overview).

The project is stored at `include/great_expectations`.

## DAG
To copy/upload local files stored at `data/*` to GCS and create table from those GCS files.
![dag-task-3](images/dag-task-3.png)

## Expectations
### Inventory
![inventory](images/expectations-inventory.png)

### Products
![products](images/expectations-products.png)

### Sales
![sales](images/expectations-sales.png)

### Stores
![stores](images/expectations-stores.png)

## Checkpoint Result Samples
![checkpoint-1](images/sample-checkpoint-result-task31.png)
![checkpoint-2](images/sample-checkpoint-result-task32.png)
![checkppint-3](images/sample-checkpoint-result-task-33.png)