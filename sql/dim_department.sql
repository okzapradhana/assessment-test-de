INSERT INTO public.dim_department
SELECT
    MD5(department_id::VARCHAR(255)) AS department_key,
    department_id,
    department_name
FROM staging.raw_department;