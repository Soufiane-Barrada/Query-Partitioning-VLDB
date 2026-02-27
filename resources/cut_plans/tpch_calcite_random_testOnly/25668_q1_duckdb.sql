SELECT COALESCE("t"."p_name", "t"."p_name") AS "P_NAME", "supplier"."s_name", "nation"."n_name", COUNT(DISTINCT "supplier"."s_suppkey") AS "UNIQUE_SUPPLIERS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("t"."p_retailprice") AS "AVERAGE_RETAIL_PRICE", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ' | Nation: ', "nation"."n_name")) AS "SUPPLIER_INFO"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 1 AND "p_size" <= 50 AND "p_comment" LIKE '%excellent%') AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t"."p_name", "supplier"."s_name", "nation"."n_name"