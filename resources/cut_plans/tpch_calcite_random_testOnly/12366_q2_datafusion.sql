SELECT COALESCE("t3"."NATION_NAME", "t3"."NATION_NAME") AS "NATION_NAME", "t3"."REGION_NAME", "t3"."TOTAL_REVENUE"
FROM (SELECT "nation"."n_name", "region"."r_name", ANY_VALUE("nation"."n_name") AS "NATION_NAME", ANY_VALUE("region"."r_name") AS "REGION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."lineitem"
INNER JOIN ("s1" INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey") ON "s1"."o_custkey" = "customer"."c_custkey") ON "lineitem"."l_orderkey" = "s1"."o_orderkey"
GROUP BY "nation"."n_name", "region"."r_name"
ORDER BY 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"