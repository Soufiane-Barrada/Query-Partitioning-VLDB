SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "region"."r_name", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2"
FROM "TPCH"."lineitem"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01') AS "t" ON "customer"."c_custkey" = "t"."o_custkey") ON "lineitem"."l_orderkey" = "t"."o_orderkey"