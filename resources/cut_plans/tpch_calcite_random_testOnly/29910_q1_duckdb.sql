SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "SUPPLIER_NAME", COUNT(DISTINCT "part"."p_name") AS "TOTAL_PARTS", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_INVENTORY_VALUE", LISTAGG("part"."p_name", ', ') AS "PART_NAMES_LIST", LISTAGG(DISTINCT "part"."p_brand", ', ') AS "UNIQUE_BRANDS"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_name"
HAVING COUNT(DISTINCT "part"."p_name") > 3