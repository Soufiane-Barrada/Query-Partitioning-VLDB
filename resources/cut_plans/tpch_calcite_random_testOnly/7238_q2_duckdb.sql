SELECT COALESCE("t5"."NATION_NAME", "t5"."NATION_NAME") AS "NATION_NAME", "t5"."TOTAL_REVENUE", "t5"."UNIQUE_CUSTOMERS", "t5"."TOTAL_ORDERS", "t5"."AVG_ORDER_VALUE"
FROM (SELECT "s1"."n_name", ANY_VALUE("s1"."n_name") AS "NATION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "s1"."c_custkey") AS "UNIQUE_CUSTOMERS", COUNT(DISTINCT "s1"."o_orderkey") AS "TOTAL_ORDERS", AVG("s1"."o_totalprice") AS "AVG_ORDER_VALUE"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN ("s1" INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey") ON "s1"."o_orderkey" = "lineitem"."l_orderkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey" AND "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
GROUP BY "s1"."n_name"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 1000000.0000
ORDER BY 3 DESC NULLS FIRST) AS "t5"