SELECT COALESCE("t3"."SUPPLIER_NAME", "t3"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t3"."PART_NAME", "t3"."SHORT_COMMENT", "t3"."LOCATION_INFO", "t3"."TOTAL_AVAILABLE_QTY"
FROM (SELECT "supplier"."s_name", "s1"."p_name", "s1"."p_comment", "region"."r_name", "nation"."n_name", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("s1"."p_name") AS "PART_NAME", ANY_VALUE(SUBSTRING("s1"."p_comment", 1, 20)) AS "SHORT_COMMENT", ANY_VALUE(CONCAT('Region: ', "region"."r_name", ', Nation: ', "nation"."n_name")) AS "LOCATION_INFO", SUM("s1"."ps_availqty") AS "TOTAL_AVAILABLE_QTY"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "supplier"."s_name", "s1"."p_name", "s1"."p_comment", "region"."r_name", "nation"."n_name"
HAVING SUM("s1"."ps_availqty") > 50
ORDER BY 9, 7) AS "t3"