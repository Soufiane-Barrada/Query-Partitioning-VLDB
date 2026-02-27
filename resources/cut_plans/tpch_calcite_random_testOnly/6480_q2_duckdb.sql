SELECT COALESCE("t6"."NATION_NAME", "t6"."NATION_NAME") AS "NATION_NAME", "t6"."REGION_NAME0", "t6"."AVG_SUPPLY_COST", "t6"."TOTAL_AVAILABLE_QUANTITY", "t6"."PART_COUNT"
FROM (SELECT "s1"."NATION_NAME", "s1"."REGION_NAME" AS "REGION_NAME0", AVG("t4"."PS_SUPPLYCOST") AS "AVG_SUPPLY_COST", SUM("t4"."PS_AVAILQTY") AS "TOTAL_AVAILABLE_QUANTITY", COUNT(DISTINCT "t4"."P_NAME") AS "PART_COUNT"
FROM (SELECT "part"."p_name" AS "P_NAME", "t3"."ps_availqty" AS "PS_AVAILQTY", "t3"."ps_supplycost" AS "PS_SUPPLYCOST", "t3"."ps_comment" AS "PS_COMMENT", "region0"."r_name" AS "REGION_NAME"
FROM "TPCH"."region" AS "region0"
INNER JOIN "TPCH"."nation" AS "nation0" ON "region0"."r_regionkey" = "nation0"."n_regionkey"
INNER JOIN "TPCH"."supplier" AS "supplier0" ON "nation0"."n_nationkey" = "supplier0"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN (SELECT *
FROM "TPCH"."partsupp"
WHERE "ps_availqty" > 0) AS "t3" ON "part"."p_partkey" = "t3"."ps_partkey") ON "supplier0"."s_suppkey" = "t3"."ps_suppkey") AS "t4"
RIGHT JOIN "s1" ON "t4"."REGION_NAME" = "s1"."REGION_NAME"
GROUP BY "s1"."NATION_NAME", "s1"."REGION_NAME"
ORDER BY "s1"."REGION_NAME", 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t6"