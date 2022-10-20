with intermediate_invoice_date as (
    SELECT
        DISTINCT
            "InvoiceId" as invoice_id,
            "CustomerId" as customer_id,
            dd.date_key as invoice_date_key,
            "BillingAddress" as billing_address,
            "BillingCity" as billing_city,
            "BillingState" as billing_state,
            "BillingCountry" as billing_country,
            "BillingPostalCode" as billing_postal_code,
            "Total" as total
    FROM 
        staging.raw_invoice i left join public.dim_date dd 
        on i."InvoiceDate" = dd.date
),
intermediate_invoice_date_customer as (
	select
		distinct
	        invoice_id,
	        c.customer_key,
	        invoice_date_key,
	        billing_address,
	        billing_city,
	        billing_state,
	        billing_country,
	        billing_postal_code,
	        total
	from intermediate_invoice_date id
	left join public.dim_customer c on id.customer_id = c.customer_id 
),
fact_invoice as (
	select 
		distinct 
	        invoice_id,
	        invoice_date_key,
	        customer_key,
			inv_d.invoice_detail_key,
	        total
	from intermediate_invoice_date_customer idc
	left join public.dim_invoice_detail inv_d on
		CONCAT(idc.billing_city, 
				idc.billing_address, 
				idc.billing_state, 
				idc.billing_country, 
				idc.billing_postal_code) = 
		CONCAT(inv_d.billing_city, 
				inv_d.billing_address, 
				inv_d.billing_state, 
				inv_d.billing_country, 
				inv_d.billing_postal_code)
)
INSERT INTO public.fact_invoice
select * from fact_invoice;