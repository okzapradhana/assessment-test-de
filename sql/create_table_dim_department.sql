DROP TABLE IF EXISTS public.dim_department;

CREATE TABLE public.dim_department(
    department_key TEXT PRIMARY KEY,
    department_id INTEGER UNIQUE,
    department_name TEXT
);

CREATE INDEX dim_department_department_id
  ON dim_department(department_id);