SELECT COALESCE("part"."p_brand", "part"."p_brand") AS "P_BRAND", "part"."p_type" AS "P_TYPE", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2", "t"."o_orderkey"
FROM "TPCH"."part"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t" INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey"