SELECT COALESCE("region"."r_name", "region"."r_name") AS "r_name", "nation"."n_name", "t"."p_comment", "t"."p_retailprice", SUM(CASE WHEN POSITION('special' IN LOWER("t"."p_comment")) > 0 THEN 1 ELSE 0 END) AS "SPECIAL_PART_COUNT", AVG("t"."p_retailprice") AS "AVERAGE_RETAIL_PRICE", COUNT(DISTINCT "supplier"."s_suppkey") AS "UNIQUE_SUPPLIERS_COUNT", ANY_VALUE("region"."r_name") AS "REGION_NAME", ANY_VALUE("nation"."n_name") AS "NATION_NAME"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "region"."r_name", "nation"."n_name", "t"."p_comment", "t"."p_retailprice"