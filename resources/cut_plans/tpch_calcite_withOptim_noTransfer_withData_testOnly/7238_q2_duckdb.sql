SELECT COALESCE("t5"."NATION_NAME", "t5"."NATION_NAME") AS "NATION_NAME", "t5"."TOTAL_REVENUE", "t5"."UNIQUE_CUSTOMERS", "t5"."TOTAL_ORDERS", "t5"."AVG_ORDER_VALUE"
FROM (SELECT "t0"."n_name", ANY_VALUE("t0"."n_name") AS "NATION_NAME", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", COUNT(DISTINCT "t1"."o_orderkey") AS "TOTAL_ORDERS", AVG("t1"."o_totalprice") AS "AVG_ORDER_VALUE"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN ((SELECT *
FROM "TPCH"."nation"
WHERE "n_name" IN ('Germany', 'Japan', 'USA')) AS "t0" INNER JOIN "TPCH"."region" ON "t0"."n_regionkey" = "region"."r_regionkey" INNER JOIN "TPCH"."customer" ON "t0"."n_nationkey" = "customer"."c_nationkey" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey" INNER JOIN "s1" ON "t1"."o_orderkey" = "s1"."l_orderkey") ON "partsupp"."ps_partkey" = "s1"."l_partkey" AND "partsupp"."ps_suppkey" = "s1"."s_suppkey"
GROUP BY "t0"."n_name"
HAVING SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) > 1000000.0000
ORDER BY 3 DESC NULLS FIRST) AS "t5"