INSERT INTO public.dim_invoice_detail
SELECT
    DISTINCT
        MD5(COALESCE("BillingCity", 0::varchar(1)) 
            || COALESCE("BillingState", 0::varchar(1))
            || COALESCE("BillingCountry", 0::varchar(1))
            || COALESCE("BillingPostalCode", 0::varchar(1))) as key,
        "BillingAddress" AS billing_address,
        "BillingCity" AS billing_city,
        "BillingState" AS billing_state,
        "BillingCountry" AS billing_country,
        "BillingPostalCode" AS billing_postal_code
FROM staging.raw_invoice;