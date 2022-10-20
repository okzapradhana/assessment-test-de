DROP TABLE IF EXISTS public.dim_invoice_detail;
CREATE TABLE public.dim_invoice_detail (
    invoice_detail_key TEXT PRIMARY KEY,
    billing_address TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_country TEXT,
    billing_postal_code TEXT
);