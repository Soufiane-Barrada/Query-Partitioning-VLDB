SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "SUPPLIER_NAME", COUNT(*) AS "TOTAL_PRODUCTS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", LISTAGG(DISTINCT "part"."p_brand", ', ') AS "BRANDS_OFFERED", LISTAGG(DISTINCT "part"."p_container", ', ') AS "CONTAINER_TYPES", CONCAT('Supplier: ', "supplier"."s_name", ' offers ', CAST(COUNT(*) AS VARCHAR CHARACTER SET "ISO-8859-1"), ' products, with an average supply cost of $', CAST(ROUND(AVG("partsupp"."ps_supplycost"), 2) AS VARCHAR CHARACTER SET "ISO-8859-1"), '.') AS "SUPPLIER_SUMMARY"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
GROUP BY "supplier"."s_name"
HAVING SUM("partsupp"."ps_availqty") > 0