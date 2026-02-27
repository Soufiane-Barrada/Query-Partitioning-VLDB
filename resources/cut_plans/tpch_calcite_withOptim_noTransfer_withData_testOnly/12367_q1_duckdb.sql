SELECT COALESCE("t"."l_orderkey", "t"."l_orderkey") AS "L_ORDERKEY", "orders"."o_orderdate" AS "O_ORDERDATE", "nation"."n_name" AS "N_NAME", "region"."r_name" AS "R_NAME", SUM("t"."l_extendedprice" * (1 - "t"."l_discount")) AS "REVENUE"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey"
INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1996-01-01') AS "t" ON "orders"."o_orderkey" = "t"."l_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "t"."l_orderkey", "orders"."o_orderdate", "nation"."n_name", "region"."r_name"