SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", "supplier"."s_name", "customer"."c_name", "orders"."o_orderdate" AS "O_ORDERDATE", "part"."p_comment", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_5", "orders"."o_orderkey", LEFT("part"."p_comment", 10) AS "FD_COL_7", CONCAT('Supplier ', "supplier"."s_name", ' provides ', "part"."p_name") AS "FD_COL_8"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
WHERE "part"."p_type" LIKE '%BRASS%' AND "orders"."o_orderdate" >= DATE '1997-01-01' AND "orders"."o_orderdate" < DATE '1997-12-31'