DROP TABLE IF EXISTS public.dim_employee;

CREATE TABLE public.dim_employee(
    employee_key TEXT PRIMARY KEY,
    employee_id INTEGER UNIQUE,
    employee_hire_date_key INTEGER,
    employee_department_key TEXT,
    employee_job_key TEXT,
    employee_first_name TEXT,
    employee_last_name TEXT,
    employee_email TEXT,
    employee_phone_number TEXT,
    employee_salary FLOAT,
    commission_pct FLOAT
);

CREATE INDEX dim_employee_employee_id
    ON dim_employee(employee_id)