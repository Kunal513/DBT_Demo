

select 
CAST(TURBO.LIFNR AS STRING) as Vendor_code,
CASE WHEN TURBO.LOEVM='X' THEN 'INACTIVE' ELSE 'ACTIVE' END as Vendor_status,
TURBO.LAND1 as Country,
TURBO.NAME1 as Vendor_name,
TURBO.ORT01 as City,
TURBO.ORT02 as District,
TURBO.PSTLZ as PO_BOX_NO,
TURBO.REGIO as Registration_NO,
TURBO.STRAS as Address_line1,
TURBO.ADRNR as Address_line2,
CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(TURBO.ERDAT,'yyyymmdd'),'yyyy-mm-dd') AS DATE) as Created_Date,
TURBO.KTOKK as Vendor_Group,
TURBO.TELF1 as Phone_NO,
TURBO.STCEG as VAT_NO,
'SAP' as Source_Nm,
TURBO.ERNAM as Created_By from {{ ref('stg_turbo_lfa1') }} TURBO where iscurrent=1
union all
select 
CAST(IGK.LIFNR AS STRING) as Vendor_code,
CASE WHEN IGK.LOEVM='X' THEN 'INACTIVE' ELSE 'ACTIVE' END as Vendor_status,
IGK.LAND1 as Country,
IGK.NAME1 as Vendor_name,
IGK.ORT01 as City,
IGK.ORT02 as District,
IGK.PSTLZ as PO_BOX_NO,
IGK.REGIO as Registration_NO,
IGK.STRAS as Address_line1,
IGK.ADRNR as Address_line2,
CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(IGK.ERDAT,'yyyymmdd'),'yyyy-mm-dd') AS DATE) as Created_Date,
IGK.KTOKK as Vendor_Group,
IGK.TELF1 as Phone_NO,
IGK.STCEG as VAT_NO,
'IGK' as Source_Nm,
IGK.ERNAM as Created_By from {{ ref('stg_igk_lfa1') }} IGK where iscurrent=1
