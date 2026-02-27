SELECT COALESCE("lineitem"."l_orderkey", "lineitem"."l_orderkey") AS "L_ORDERKEY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE", "orders"."o_orderdate" AS "O_ORDERDATE", "nation"."n_name" AS "N_NAME", "region"."r_name" AS "R_NAME"
FROM "TPCH"."lineitem"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
INNER JOIN "TPCH"."nation" ON "customer"."c_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "lineitem"."l_shipdate" >= '1995-01-01' AND "lineitem"."l_shipdate" < '1996-01-01'
GROUP BY "lineitem"."l_orderkey", "orders"."o_orderdate", "nation"."n_name", "region"."r_name"