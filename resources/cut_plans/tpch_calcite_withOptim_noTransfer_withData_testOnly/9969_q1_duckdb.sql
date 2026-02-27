SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "region"."r_name", ANY_VALUE("nation"."n_name") AS "NATION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", ANY_VALUE("region"."r_name") AS "REGION_NAME"
FROM "TPCH"."lineitem"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1998-01-01') AS "t" INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey") ON "t"."o_custkey" = "customer"."c_custkey") ON "lineitem"."l_orderkey" = "t"."o_orderkey"
GROUP BY "nation"."n_name", "region"."r_name"
HAVING SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) > 100000.0000