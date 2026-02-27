SELECT COALESCE("t5"."CUSTOMER_NAME", "t5"."CUSTOMER_NAME") AS "CUSTOMER_NAME", "t5"."TOTAL_REVENUE", "t5"."NATION_NAME", "t5"."O_ORDERDATE", "t5"."O_ORDERDATE" AS "o_orderdate_"
FROM (SELECT "customer"."c_name", "s1"."n_name", "t1"."o_orderdate" AS "O_ORDERDATE", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("s1"."n_name") AS "NATION_NAME"
FROM "s1"
INNER JOIN "TPCH"."customer" ON "s1"."n_nationkey" = "customer"."c_nationkey"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t1" ON "customer"."c_custkey" = "t1"."o_custkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "supplier"."s_suppkey") ON "t1"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "customer"."c_name", "s1"."n_name", "t1"."o_orderdate"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 100000.0000
ORDER BY 5 DESC NULLS FIRST, "t1"."o_orderdate" DESC NULLS FIRST) AS "t5"