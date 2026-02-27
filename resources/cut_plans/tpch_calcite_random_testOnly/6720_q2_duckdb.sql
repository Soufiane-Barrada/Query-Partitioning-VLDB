SELECT COALESCE("t4"."NATION_NAME", "t4"."NATION_NAME") AS "NATION_NAME", "t4"."TOTAL_REVENUE", "t4"."TOTAL_ORDERS", "t4"."AVG_CUSTOMER_BALANCE"
FROM (SELECT "nation"."n_name", ANY_VALUE("nation"."n_name") AS "NATION_NAME", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "s1"."o_orderkey") AS "TOTAL_ORDERS", AVG("customer"."c_acctbal") AS "AVG_CUSTOMER_BALANCE"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN "s1" ON "customer"."c_custkey" = "s1"."o_custkey") ON "partsupp"."ps_partkey" = "s1"."l_partkey" AND "supplier"."s_suppkey" = "s1"."l_suppkey"
GROUP BY "nation"."n_name"
HAVING SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) > 1000000.0000
ORDER BY 3 DESC NULLS FIRST) AS "t4"