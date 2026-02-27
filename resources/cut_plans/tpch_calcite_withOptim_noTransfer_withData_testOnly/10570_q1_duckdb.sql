SELECT COALESCE("t0"."l_orderkey", "t0"."l_orderkey") AS "L_ORDERKEY", SUM("t0"."l_extendedprice" * (1 - "t0"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."customer"
WHERE "c_mktsegment" = 'BUILDING') AS "t" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1996-01-01') AS "t0" ON "orders"."o_orderkey" = "t0"."l_orderkey") ON "t"."c_custkey" = "orders"."o_custkey") ON "partsupp"."ps_partkey" = "t0"."l_partkey" AND "supplier"."s_suppkey" = "t0"."l_suppkey"
GROUP BY "t0"."l_orderkey"