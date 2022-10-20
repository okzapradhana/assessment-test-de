INSERT INTO public.dim_job
SELECT
    MD5(job_id) AS job_key,
    job_id,
    job_title,
    min_salary,
    max_salary
FROM staging.raw_job;