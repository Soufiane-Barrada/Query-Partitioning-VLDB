SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", COUNT(DISTINCT "supplier"."s_suppkey") AS "UNIQUE_SUPPLIERS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("part"."p_retailprice") AS "AVERAGE_RETAIL_PRICE", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ' | Nation: ', "nation"."n_name")) AS "SUPPLIER_INFO"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
WHERE "part"."p_size" >= 1 AND "part"."p_size" <= 50 AND "part"."p_comment" LIKE '%excellent%'
GROUP BY "part"."p_name", "supplier"."s_name", "nation"."n_name"
HAVING COUNT(DISTINCT "supplier"."s_suppkey") > 1