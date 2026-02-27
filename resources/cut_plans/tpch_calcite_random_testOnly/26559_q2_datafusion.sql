SELECT COALESCE("t4"."SUPPLIER_INFO", "t4"."SUPPLIER_INFO") AS "SUPPLIER_INFO", "t4"."TOTAL_PARTS_SUPPLIED", "t4"."TOTAL_AVAILABLE_QUANTITY", "t4"."AVERAGE_SUPPLY_COST", "t4"."PARTS_DETAILS", "t4"."REGION_NAME"
FROM (SELECT "s1"."s_suppkey", "s1"."s_name", "s1"."r_name", ANY_VALUE(CONCAT('Supplier: ', "s1"."s_name", ' (', CAST("s1"."s_suppkey" AS VARCHAR CHARACTER SET "ISO-8859-1"), ')')) AS "SUPPLIER_INFO", COUNT(DISTINCT "partsupp"."ps_partkey") AS "TOTAL_PARTS_SUPPLIED", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", LISTAGG(DISTINCT CONCAT('Part: ', "part"."p_name", ' [', CAST("part"."p_partkey" AS VARCHAR CHARACTER SET "ISO-8859-1"), '] - Price: $', CAST("part"."p_retailprice" AS VARCHAR CHARACTER SET "ISO-8859-1")), ', ') AS "PARTS_DETAILS", ANY_VALUE("s1"."r_name") AS "REGION_NAME"
FROM "s1"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."s_suppkey", "s1"."s_name", "s1"."r_name"
HAVING COUNT(DISTINCT "partsupp"."ps_partkey") > 5
ORDER BY 6 DESC NULLS FIRST) AS "t4"