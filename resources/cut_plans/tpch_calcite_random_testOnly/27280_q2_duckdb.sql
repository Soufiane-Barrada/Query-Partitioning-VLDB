SELECT COALESCE("t4"."SPECIAL_PART_COUNT", "t4"."SPECIAL_PART_COUNT") AS "SPECIAL_PART_COUNT", "t4"."AVERAGE_RETAIL_PRICE", "t4"."UNIQUE_SUPPLIERS_COUNT", "t4"."REGION_NAME", "t4"."NATION_NAME"
FROM (SELECT "s1"."r_name", "s1"."n_name", "t0"."p_comment", "t0"."p_retailprice", SUM(CASE WHEN POSITION('special' IN LOWER("t0"."p_comment")) > 0 THEN 1 ELSE 0 END) AS "SPECIAL_PART_COUNT", AVG("t0"."p_retailprice") AS "AVERAGE_RETAIL_PRICE", COUNT(DISTINCT "supplier"."s_suppkey") AS "UNIQUE_SUPPLIERS_COUNT", ANY_VALUE("s1"."r_name") AS "REGION_NAME", ANY_VALUE("s1"."n_name") AS "NATION_NAME"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."r_name", "s1"."n_name", "t0"."p_comment", "t0"."p_retailprice"
HAVING COUNT(DISTINCT "supplier"."s_suppkey") > 5
ORDER BY 8, 9) AS "t4"