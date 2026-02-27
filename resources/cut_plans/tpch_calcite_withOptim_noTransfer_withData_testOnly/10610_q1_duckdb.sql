SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "region"."r_name", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t" ON "customer"."c_custkey" = "t"."o_custkey"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey") ON "t"."o_orderkey" = "lineitem"."l_orderkey"