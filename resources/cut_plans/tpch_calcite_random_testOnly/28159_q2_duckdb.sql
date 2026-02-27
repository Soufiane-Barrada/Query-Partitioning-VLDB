SELECT COALESCE("t5"."SUPPLIER_NAME", "t5"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t5"."TOTAL_PARTS_SUPPLIED", "t5"."TOTAL_AVAILABLE_QUANTITY", "t5"."SUPPLIED_PART_NAMES", "t5"."REGION_NAME"
FROM (SELECT "t1"."s_name", "region"."r_name", ANY_VALUE("t1"."s_name") AS "SUPPLIER_NAME", COUNT(DISTINCT "s1"."p_partkey") AS "TOTAL_PARTS_SUPPLIED", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", LISTAGG(DISTINCT "s1"."p_name", ', ') AS "SUPPLIED_PART_NAMES", ANY_VALUE("region"."r_name") AS "REGION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 500.00) AS "t1" ON "nation"."n_nationkey" = "t1"."s_nationkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t1"."s_name", "region"."r_name"
HAVING COUNT(DISTINCT "s1"."p_partkey") > 5
ORDER BY 4 DESC NULLS FIRST, 3) AS "t5"