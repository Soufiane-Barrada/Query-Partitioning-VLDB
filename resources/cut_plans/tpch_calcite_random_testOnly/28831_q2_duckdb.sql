SELECT COALESCE("t6"."P_NAME", "t6"."P_NAME") AS "P_NAME", "t6"."S_NAME", "t6"."SUPPLIER_INFO", "t6"."TOTAL_REVENUE"
FROM (SELECT "t2"."p_name" AS "P_NAME", "t1"."s_name" AS "S_NAME", "region"."r_name", "nation"."n_name", ANY_VALUE(CONCAT('Region: ', "region"."r_name", ', Nation: ', "nation"."n_name", ', Supplier: ', "t1"."s_name")) AS "SUPPLIER_INFO", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" NOT LIKE '%fraud%') AS "t1" ON "nation"."n_nationkey" = "t1"."s_nationkey" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE 'prod_%') AS "t2" INNER JOIN "TPCH"."partsupp" ON "t2"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."s_suppkey" = "partsupp"."ps_suppkey") ON "s1"."l_partkey" = "t2"."p_partkey"
GROUP BY "t2"."p_name", "t1"."s_name", "region"."r_name", "nation"."n_name"
HAVING SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) > 1000.0000
ORDER BY 6 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t6"