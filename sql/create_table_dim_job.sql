DROP TABLE IF EXISTS public.dim_job;

CREATE TABLE public.dim_job(
    job_key TEXT PRIMARY KEY,
    job_id TEXT UNIQUE,
    job_title TEXT,
    min_salary FLOAT,
    max_salary FLOAT
);

CREATE INDEX dim_job_job_id
  ON dim_job(job_id);