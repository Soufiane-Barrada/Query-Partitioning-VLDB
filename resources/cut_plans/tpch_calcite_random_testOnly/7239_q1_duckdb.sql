SELECT COALESCE("region"."r_name", "region"."r_name") AS "r_name", "t2"."l_extendedprice" * (1 - "t2"."l_discount") AS "FD_COL_1", "orders"."o_orderkey", "customer"."c_custkey", "t1"."S_SUPPKEY"
FROM (SELECT "supplier"."s_suppkey" AS "S_SUPPKEY", "supplier"."s_name" AS "S_NAME", COUNT(*) AS "TOTALSUPPLIED", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTALSUPPLYCOST"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name"
HAVING COUNT(*) > 5) AS "t1"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-04-01' AND "l_shipdate" < DATE '1997-10-01') AS "t2" INNER JOIN "TPCH"."orders" ON "t2"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "t1"."S_SUPPKEY" = "t2"."l_suppkey"