

-- Use the `ref` function to select from other models
select Vendor_code from {{ ref('my_first_dbt_model') }}
