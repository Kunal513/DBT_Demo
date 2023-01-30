SELECT * from {{ ref("SAP_TURBO_BOM_VW") }} 
UNION ALL
SELECT * from {{ ref("SAP_IGK_BOM_VW") }} 
UNION ALL
SELECT * from {{ ref("SAP_SUNRISE_BOM_VW") }}  