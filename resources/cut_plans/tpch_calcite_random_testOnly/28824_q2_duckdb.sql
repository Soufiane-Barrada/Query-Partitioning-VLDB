SELECT COALESCE("t3"."PART_INFO", "t3"."PART_INFO") AS "PART_INFO", "t3"."SUPPLIER_COUNT", "t3"."NATIONS_SUPPLIED", "t3"."UNIQUE_SUPPLIERS", "t3"."AVG_SUPPLY_COST"
FROM (SELECT CONCAT('Part Name: ', "s1"."p_name", ', Brand: ', "s1"."p_brand", ', Type: ', "s1"."p_type", ', Size: ', CAST("s1"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1")) AS "PART_INFO", COUNT(*) AS "SUPPLIER_COUNT", LISTAGG(DISTINCT "nation"."n_name", ', ') AS "NATIONS_SUPPLIED", LISTAGG(DISTINCT "supplier"."s_name", ', ') AS "UNIQUE_SUPPLIERS", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY CONCAT('Part Name: ', "s1"."p_name", ', Brand: ', "s1"."p_brand", ', Type: ', "s1"."p_type", ', Size: ', CAST("s1"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1"))
ORDER BY 2 DESC NULLS FIRST, 5
FETCH NEXT 10 ROWS ONLY) AS "t3"