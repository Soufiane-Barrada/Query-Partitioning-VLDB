SELECT COALESCE(CONCAT('Part Name: ', "t"."p_name", ', Brand: ', "t"."p_brand", ', Type: ', "t"."p_type", ', Size: ', CAST("t"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1")), CONCAT('Part Name: ', "t"."p_name", ', Brand: ', "t"."p_brand", ', Type: ', "t"."p_type", ', Size: ', CAST("t"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1"))) AS "PART_INFO", COUNT(*) AS "SUPPLIER_COUNT", LISTAGG(DISTINCT "nation"."n_name", ', ') AS "NATIONS_SUPPLIED", LISTAGG(DISTINCT "supplier"."s_name", ', ') AS "UNIQUE_SUPPLIERS", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY CONCAT('Part Name: ', "t"."p_name", ', Brand: ', "t"."p_brand", ', Type: ', "t"."p_type", ', Size: ', CAST("t"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1"))