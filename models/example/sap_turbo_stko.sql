{{ config(materialized="table") }}

select  
    mandt,
    stlty,
    stlnr,
    stlal,
    stkoz,
    datuv,
    loekz,
    bmein,
    bmeng,
    stlst,
    valid_to,
    andat,
    'STKO' as source_nm,
    effective_date_from
from edap_stg.turbo_stko
