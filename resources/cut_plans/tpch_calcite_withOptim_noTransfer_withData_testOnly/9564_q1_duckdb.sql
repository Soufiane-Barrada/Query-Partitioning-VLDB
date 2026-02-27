SELECT COALESCE("customer"."c_name", "customer"."c_name") AS "c_name", "nation"."n_name", "t0"."o_orderdate" AS "O_ORDERDATE", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("nation"."n_name") AS "NATION_NAME"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'EUROPE') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "supplier"."s_suppkey") ON "t0"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "customer"."c_name", "nation"."n_name", "t0"."o_orderdate"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 100000.0000