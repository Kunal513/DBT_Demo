{{ config(materialized="table") }}

select
    base64(concat(trim(a.stlnr), "/@/", trim(a.werks), "/@/", trim(a.stlal))) bom_id,
    a.stlal alternate_bom_number,
    "SAP_IGK" source,
    if(b.stlty = 'M', "MANUFACTURING", "ALTERNATE") bom_type,
    if(b.stlst = '01' or b.stlst = '03', "ACTIVE", "INACTIVE") bom_status,
    b.bmein bom_base_uom,
    cast(b.bmeng as float) bom_base_quantity,
    coalesce(
        cast(
            concat(
                left(b.datuv, 4), '-', substring(b.datuv, 5, 2), '-', right(b.datuv, 2)
            ) as date
        ),
        to_date("1900-01-01", "yyyy-MM-dd")
    ) bom_effective_from,
    coalesce(
        lag(
            cast(
                concat(
                    left(b.datuv, 4),
                    '-',
                    substring(b.datuv, 5, 2),
                    '-',
                    right(b.datuv, 2)
                ) as date
            )
        ) over (
            partition by concat(a.stlnr, "/@/", a.werks, "/@/", a.stlal), trim(a.mandt)
            order by
                cast(
                    concat(
                        left(b.datuv, 4),
                        '-',
                        substring(b.datuv, 5, 2),
                        '-',
                        right(b.datuv, 2)
                    ) as date
                ) desc,
                cast(
                    concat(
                        left(b.andat, 4),
                        '-',
                        substring(b.andat, 5, 2),
                        '-',
                        right(b.andat, 2)
                    ) as date
                ) desc,
                cast(b.stkoz as long) desc,
                b.loekz,
                b.lkenz
        ),
        to_date("2099-01-01", "yyyy-MM-dd")
    ) bom_effective_to,
    b.bmein uom_id,
    a.werks erp_site_code,
    a.matnr source_item_code,
    concat(trim(a.stlnr), "/@/", trim(a.werks), "/@/", trim(a.stlal)) source_bom_code,
    cast(b.stkoz as long) bom_counter,
    if(b.loekz = "X", true, if(b.lkenz = "X", true, false)) is_deleted,
    a.matnr material_id_lookup,
    '' prntitmnbr,
    to_timestamp(
        coalesce(
            case
                when a.andat = ''
                then null
                else
                    cast(
                        concat(
                            left(a.andat, 4),
                            '-',
                            substring(a.andat, 5, 2),
                            '-',
                            right(a.andat, 2)
                        ) as date
                    )
            end,
            current_timestamp()
        )
    ) as source_insert_ts
from {{ ref("sap_igk_mast") }}  a
inner join
    {{ ref("sap_igk_stko") }}  b
    on a.stlnr = b.stlnr
    and a.stlal = b.stlal
    and cast(a.mandt as int) = cast(b.mandt as int)
where if(b.stlty = 'M', "MANUFACTURING", "ALTERNATE") = 'MANUFACTURING'
order by
    base64(concat(a.stlnr, "/@/", a.werks, "/@/", a.stlal)),
    to_date(b.datuv, "yyyy-MM-dd") desc,
    cast(b.stkoz as long) desc
