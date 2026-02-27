SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "P_PARTKEY", "part"."p_name" AS "P_NAME", "supplier"."s_name" AS "S_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "orders"."o_orderdate" >= DATE '1995-01-01' AND "orders"."o_orderdate" < DATE '1995-01-31'
GROUP BY "part"."p_partkey", "part"."p_name", "supplier"."s_name"