gcloud auth activate-service-account --key-file=/opt/airflow/telkom-assessment-service-account.json --project=static-gravity-312212

gsutil cp /opt/airflow/data/dataset/2021-10-10/2021-10-10-inventory.csv gs://data-quality-datasets/inventory.csv
gsutil cp /opt/airflow/data/dataset/2021-10-10/2021-10-10-products.csv gs://data-quality-datasets/products.csv
gsutil cp /opt/airflow/data/dataset/2021-10-10/2021-10-10-sales.csv gs://data-quality-datasets/sales.csv
gsutil cp /opt/airflow/data/dataset/2021-10-10/2021-10-10-stores.csv gs://data-quality-datasets/stores.csv

gsutil cp /opt/airflow/data/dataset/2021-10-11/2021-10-11-inventory.csv gs://data-quality-datasets/new_inventory.csv
gsutil cp /opt/airflow/data/dataset/2021-10-11/2021-10-11-products.csv gs://data-quality-datasets/new_products.csv
gsutil cp /opt/airflow/data/dataset/2021-10-11/2021-10-11-sales.csv gs://data-quality-datasets/new_sales.csv
gsutil cp /opt/airflow/data/dataset/2021-10-11/2021-10-11-stores.csv gs://data-quality-datasets/new_stores.csv