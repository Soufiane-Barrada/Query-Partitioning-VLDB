SELECT COALESCE("t"."p_name", "t"."p_name") AS "P_NAME", "supplier"."s_name" AS "S_NAME", SUM("t0"."l_quantity") AS "TOTAL_QUANTITY", AVG("t0"."l_extendedprice") AS "AVERAGE_PRICE", COUNT(DISTINCT "customer"."c_custkey") AS "CUSTOMER_COUNT", LISTAGG(DISTINCT "nation"."n_name", '; ') AS "NATIONS_SERVED"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10 AND ("p_retailprice" >= 50.00 AND "p_retailprice" <= 100.00)) AS "t"
INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01') AS "t0" ON "customer"."c_custkey" = "t0"."l_orderkey") ON "t"."p_partkey" = "t0"."l_partkey"
GROUP BY "t"."p_name", "supplier"."s_name"
HAVING SUM("t0"."l_quantity") > 100.00