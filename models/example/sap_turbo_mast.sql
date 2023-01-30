
{{ config(materialized="table") }}


select
    mandt,
    matnr,
    werks,
    stlan,
    stlnr,
    stlal,
    andat,
    'MAST' as source_nm,
    effective_date_from
from edap_stg.turbo_mast
where stlan = '1'
