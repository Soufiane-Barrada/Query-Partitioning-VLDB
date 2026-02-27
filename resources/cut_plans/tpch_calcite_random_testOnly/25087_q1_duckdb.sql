SELECT COALESCE("t"."p_name", "t"."p_name") AS "P_NAME", "supplier"."s_name" AS "S_NAME", "t0"."l_quantity", "orders"."o_orderkey", "t0"."l_extendedprice" * (1 - "t0"."l_discount") AS "FD_COL_4", CONCAT("customer"."c_name", ': ', CAST("orders"."o_orderdate" AS VARCHAR CHARACTER SET "ISO-8859-1"), ' - ', "t0"."l_comment") AS "FD_COL_5", '; ' AS "FD_COL_6"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%BRASS%') AS "t" INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN "TPCH"."orders" ON "t0"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t"."p_partkey" = "t0"."l_partkey") ON "supplier"."s_suppkey" = "t0"."l_suppkey"