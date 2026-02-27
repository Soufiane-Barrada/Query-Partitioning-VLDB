SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "part"."p_type" AS "P_TYPE", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2", "orders"."o_orderkey"
FROM "TPCH"."part"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "orders"."o_orderdate" >= DATE '1997-01-01' AND "orders"."o_orderdate" < DATE '1997-12-31'