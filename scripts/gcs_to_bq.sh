gcloud auth activate-service-account \
    --key-file=/opt/airflow/telkom-assessment-service-account.json \
    --project=static-gravity-312212

# load inventory table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.inventory \
    gs://data-quality-datasets/inventory.csv \
    store_id:INTEGER,product_id:INTEGER,stock_on_hand:STRING

# load new inventory table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.new_inventory \
    gs://data-quality-datasets/new_inventory.csv \
    store_id:INTEGER,product_id:INTEGER,stock_on_hand:STRING

# load products table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.products \
    gs://data-quality-datasets/products.csv \
    product_id:INTEGER,product_name:STRING,product_category:STRING,product_cost:STRING,product_price:STRING

# load new products table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.new_products \
    gs://data-quality-datasets/new_products.csv \
    product_id:INTEGER,product_name:STRING,product_category:STRING,product_cost:STRING,product_price:STRING

# load sales table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.sales \
    gs://data-quality-datasets/sales.csv \
    sale_id:INTEGER,date:STRING,store_id:INTEGER,product_id:INTEGER,units:STRING

# load new sales table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.new_sales \
    gs://data-quality-datasets/new_sales.csv \
    sale_id:INTEGER,date:STRING,store_id:INTEGER,product_id:INTEGER,units:STRING

# load stores table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.stores \
    gs://data-quality-datasets/stores.csv \
    store_id:INTEGER,store_name:STRING,store_city:STRING,store_location:STRING,store_open_date:STRING

# load new stores table
bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --location=asia-southeast2 \
    --replace=true \
    assessment_ge.new_stores \
    gs://data-quality-datasets/new_stores.csv \
    store_id:INTEGER,store_name:STRING,store_city:STRING,store_location:STRING,store_open_date:STRING