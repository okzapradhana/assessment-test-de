DROP TABLE IF EXISTS public.dim_customer;
CREATE TABLE IF NOT EXISTS public.dim_customer (
    customer_key TEXT PRIMARY KEY,
    customer_id INTEGER UNIQUE NOT NULL,
    first_name TEXT,
    last_name TEXT,
    company TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    postal_code TEXT,
    phone TEXT,
    fax TEXT,
    email TEXT,
    support_rep_id INTEGER
);

CREATE INDEX IF NOT EXISTS dim_customer_customer_id
  ON dim_customer(customer_id);