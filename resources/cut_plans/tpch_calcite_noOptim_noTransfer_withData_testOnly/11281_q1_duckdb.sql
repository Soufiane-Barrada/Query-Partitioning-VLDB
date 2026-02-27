SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "P_PARTKEY", "part"."p_name" AS "P_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", "partsupp"."ps_supplycost" AS "PS_SUPPLYCOST", "partsupp"."ps_availqty" AS "PS_AVAILQTY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "orders"."o_orderdate" >= '1997-01-01' AND "orders"."o_orderdate" < '1997-12-31'
GROUP BY "part"."p_partkey", "part"."p_name", "supplier"."s_name", "partsupp"."ps_supplycost", "partsupp"."ps_availqty"