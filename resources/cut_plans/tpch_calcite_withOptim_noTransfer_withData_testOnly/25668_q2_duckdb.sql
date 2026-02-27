SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."UNIQUE_SUPPLIERS", "t4"."TOTAL_AVAILABLE_QUANTITY", "t4"."AVERAGE_RETAIL_PRICE", "t4"."SUPPLIER_INFO"
FROM (SELECT "s1"."p_name" AS "P_NAME", "supplier"."s_name", "nation"."n_name", COUNT(DISTINCT "supplier"."s_suppkey") AS "UNIQUE_SUPPLIERS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("s1"."p_retailprice") AS "AVERAGE_RETAIL_PRICE", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ' | Nation: ', "nation"."n_name")) AS "SUPPLIER_INFO"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."p_name", "supplier"."s_name", "nation"."n_name"
HAVING COUNT(DISTINCT "supplier"."s_suppkey") > 1
ORDER BY 5 DESC NULLS FIRST, 6
FETCH NEXT 10 ROWS ONLY) AS "t4"