INSERT INTO public.dim_customer
SELECT
    DISTINCT
        MD5("CustomerId"::VARCHAR(255)) AS customer_key,
        "CustomerId" AS customer_id,
        "FirstName" AS first_name,
        "LastName" AS last_name,
        "Company" AS company,
        "Address" AS address,
        "City" AS city,
        "State" AS state,
        "Country" AS country,
        "PostalCode" AS postal_code,
        "Phone" AS phone,
        "Fax" AS fax,
        "Email" as email,
        "SupportRepId" AS support_rep_id
FROM staging.raw_customer;