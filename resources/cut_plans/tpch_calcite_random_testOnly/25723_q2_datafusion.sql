SELECT COALESCE("t5"."PART_INFO", "t5"."PART_INFO") AS "PART_INFO", "t5"."TOTAL_RETURNED_QUANTITY", "t5"."TOTAL_SALES", "t5"."TOTAL_ORDERS", "t5"."REGION_NAME", "t5"."NATION_NAME"
FROM (SELECT "t1"."p_partkey", "t1"."p_name", "t1"."p_mfgr", "t1"."p_brand", "region"."r_name", "nation"."n_name", ANY_VALUE(CONCAT('Part Name: ', "t1"."p_name", ' - Manufacturer: ', "t1"."p_mfgr", ' - Brand: ', "t1"."p_brand")) AS "PART_INFO", SUM(CASE WHEN "s1"."l_returnflag" = 'Y' THEN "s1"."l_quantity" ELSE 0.00 END) AS "TOTAL_RETURNED_QUANTITY", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "TOTAL_SALES", COUNT(DISTINCT "s1"."o_orderkey") AS "TOTAL_ORDERS", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE("nation"."n_name") AS "NATION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_comment" LIKE '%fragile%') AS "t1" INNER JOIN "s1" ON "t1"."p_partkey" = "s1"."l_partkey") ON "supplier"."s_suppkey" = "s1"."l_suppkey"
GROUP BY "t1"."p_partkey", "t1"."p_name", "t1"."p_mfgr", "t1"."p_brand", "region"."r_name", "nation"."n_name"
HAVING SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) > 10000.0000
ORDER BY 9 DESC NULLS FIRST) AS "t5"