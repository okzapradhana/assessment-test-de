with intermediate_employee_date as (
	SELECT
        DISTINCT
            employee_id,
            dd.date_key AS employee_hire_date_key,
            e.department_id,
            e.job_id,
            first_name AS employee_first_name,
            last_name AS employee_last_name,
            email AS employee_email,
            phone_number AS employee_phone_number,
            salary AS employee_salary,
            commission_pct
	FROM staging.raw_employee e
	left join public.dim_date dd on e.hire_date = dd.date 
)
INSERT INTO public.dim_employee
select 
        DISTINCT
            MD5(employee_id::VARCHAR(255)) AS employee_key,
            employee_id,
            employee_hire_date_key,
            d.department_key as employee_department_key,
            j.job_key AS employee_job_key,
            employee_first_name
            employee_last_name,
            employee_email,
            employee_phone_number,
            employee_salary,
            commission_pct
from intermediate_employee_date ed
left join public.dim_job j on ed.job_id = j.job_id 
left join public.dim_department d on ed.department_id = d.department_id;