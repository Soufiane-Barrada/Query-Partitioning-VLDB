SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "P_NAME", "t"."r_name", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", MAX(CASE WHEN LENGTH("t0"."p_comment") > 20 THEN SUBSTRING("t0"."p_comment", 1, 20) || '...' ELSE "t0"."p_comment" END) AS "TRUNCATED_COMMENT", ANY_VALUE("t"."r_name") AS "REGION_NAME"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" LIKE 'Eu%') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_name", "t"."r_name"
HAVING COUNT(DISTINCT "partsupp"."ps_suppkey") > 5