SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "s_suppkey", "supplier"."s_name", "t"."r_name", ANY_VALUE(CONCAT('Supplier: ', "supplier"."s_name", ' (', CAST("supplier"."s_suppkey" AS VARCHAR CHARACTER SET "ISO-8859-1"), ')')) AS "SUPPLIER_INFO", COUNT(DISTINCT "partsupp"."ps_partkey") AS "TOTAL_PARTS_SUPPLIED", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", LISTAGG(DISTINCT CONCAT('Part: ', "part"."p_name", ' [', CAST("part"."p_partkey" AS VARCHAR CHARACTER SET "ISO-8859-1"), '] - Price: $', CAST("part"."p_retailprice" AS VARCHAR CHARACTER SET "ISO-8859-1")), ', ') AS "PARTS_DETAILS", ANY_VALUE("t"."r_name") AS "REGION_NAME"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" LIKE '%East%') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name", "t"."r_name"