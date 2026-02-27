SELECT COALESCE(CONCAT('Part Name: ', "part"."p_name", ', Brand: ', "part"."p_brand", ', Type: ', "part"."p_type", ', Size: ', CAST("part"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1")), CONCAT('Part Name: ', "part"."p_name", ', Brand: ', "part"."p_brand", ', Type: ', "part"."p_type", ', Size: ', CAST("part"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1"))) AS "PART_INFO", COUNT(*) AS "SUPPLIER_COUNT", LISTAGG(DISTINCT "nation"."n_name", ', ') AS "NATIONS_SUPPLIED", LISTAGG(DISTINCT "supplier"."s_name", ', ') AS "UNIQUE_SUPPLIERS", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "part"."p_size" > 10
GROUP BY CONCAT('Part Name: ', "part"."p_name", ', Brand: ', "part"."p_brand", ', Type: ', "part"."p_type", ', Size: ', CAST("part"."p_size" AS VARCHAR CHARACTER SET "ISO-8859-1"))