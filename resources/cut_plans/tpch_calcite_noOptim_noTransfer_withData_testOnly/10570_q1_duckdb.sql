SELECT COALESCE("lineitem"."l_orderkey", "lineitem"."l_orderkey") AS "L_ORDERKEY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."lineitem"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
INNER JOIN "TPCH"."supplier" ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."partsupp" ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
WHERE "customer"."c_mktsegment" = 'BUILDING' AND "lineitem"."l_shipdate" >= DATE '1995-01-01' AND "lineitem"."l_shipdate" < DATE '1996-01-01'
GROUP BY "lineitem"."l_orderkey"