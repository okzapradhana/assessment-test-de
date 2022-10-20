DROP TABLE IF EXISTS public.fact_invoice;
CREATE TABLE public.fact_invoice (
    invoice_id INTEGER PRIMARY KEY,
    invoice_date INTEGER,
    customer_key TEXT,
    invoice_detail_key TEXT,
    total FLOAT
);