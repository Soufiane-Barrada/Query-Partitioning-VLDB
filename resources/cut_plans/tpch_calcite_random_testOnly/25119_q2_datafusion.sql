SELECT COALESCE("t5"."P_NAME", "t5"."P_NAME") AS "P_NAME", "t5"."S_NAME", "t5"."TOTAL_QUANTITY", "t5"."AVERAGE_PRICE", "t5"."CUSTOMER_COUNT", "t5"."NATIONS_SERVED"
FROM (SELECT "t1"."p_name" AS "P_NAME", "supplier"."s_name" AS "S_NAME", SUM("s1"."l_quantity") AS "TOTAL_QUANTITY", AVG("s1"."l_extendedprice") AS "AVERAGE_PRICE", COUNT(DISTINCT "customer"."c_custkey") AS "CUSTOMER_COUNT", LISTAGG(DISTINCT "nation"."n_name", '; ') AS "NATIONS_SERVED"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10 AND ("p_retailprice" >= 50.00 AND "p_retailprice" <= 100.00)) AS "t1"
INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN ("TPCH"."customer" INNER JOIN "s1" ON "customer"."c_custkey" = "s1"."l_orderkey") ON "t1"."p_partkey" = "s1"."l_partkey"
GROUP BY "t1"."p_name", "supplier"."s_name"
HAVING SUM("s1"."l_quantity") > 100.00
ORDER BY 3 DESC NULLS FIRST) AS "t5"