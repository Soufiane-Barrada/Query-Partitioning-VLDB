SELECT COALESCE("t3"."P_PARTKEY", "t3"."P_PARTKEY") AS "P_PARTKEY", "t3"."P_NAME", "t3"."REVENUE", "t3"."NATION", "t3"."REGION"
FROM (SELECT "part"."p_partkey" AS "P_PARTKEY", "part"."p_name" AS "P_NAME", "nation"."n_name", "region"."r_name", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "REVENUE", ANY_VALUE("nation"."n_name") AS "NATION", ANY_VALUE("region"."r_name") AS "REGION"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "s1" ON "part"."p_partkey" = "s1"."l_partkey") ON "supplier"."s_suppkey" = "s1"."l_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name", "nation"."n_name", "region"."r_name"
ORDER BY 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"