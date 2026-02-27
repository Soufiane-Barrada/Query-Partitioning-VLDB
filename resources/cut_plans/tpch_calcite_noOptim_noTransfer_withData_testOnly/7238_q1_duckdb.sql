SELECT COALESCE(ANY_VALUE("nation"."n_name"), ANY_VALUE("nation"."n_name")) AS "NATION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "customer"."c_custkey") AS "UNIQUE_CUSTOMERS", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS", AVG("orders"."o_totalprice") AS "AVG_ORDER_VALUE"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN "TPCH"."supplier" ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."partsupp" ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
INNER JOIN "TPCH"."nation" ON "customer"."c_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "orders"."o_orderdate" >= '1997-01-01' AND "orders"."o_orderdate" <= '1997-12-31' AND ("nation"."n_name" = 'USA' OR "nation"."n_name" = 'Germany' OR "nation"."n_name" = 'Japan')
GROUP BY "nation"."n_name"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 1000000.0000