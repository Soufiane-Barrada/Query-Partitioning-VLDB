SELECT COALESCE(CONCAT("t"."s_name", ' ', "t"."s_address"), CONCAT("t"."s_name", ' ', "t"."s_address")) AS "FD_COL_0", LEFT("part"."p_name", 10) AS "FD_COL_1", "region"."r_name", "customer"."c_custkey", "t0"."l_extendedprice" * (1 - "t0"."l_discount") AS "FD_COL_4"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" LIKE '%special%') AS "t" ON "nation"."n_nationkey" = "t"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" INNER JOIN "TPCH"."orders" ON "t0"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "part"."p_partkey" = "t0"."l_partkey"