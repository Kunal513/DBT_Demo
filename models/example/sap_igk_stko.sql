{{ config(materialized="table") }}

select
    mandt,
    stlty,
    stlnr,
    stlal,
    stkoz,
    replace(cast(datuv as string), '-', '') datuv,
    lkenz,
    loekz,
    bmein,
    bmeng,
    stlst,
    replace(cast(andat as string), '-', '') andat,
    'STKO' as source_nm,
    effective_date_from
from edap_stg.igk_stko
