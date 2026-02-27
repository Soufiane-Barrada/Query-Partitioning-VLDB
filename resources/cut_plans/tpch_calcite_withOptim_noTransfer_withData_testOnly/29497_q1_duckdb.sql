SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "SUPPLIER_NAME", COUNT(*) AS "TOTAL_PRODUCTS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", LISTAGG(DISTINCT "part"."p_brand", ', ') AS "BRANDS_OFFERED", LISTAGG(DISTINCT "part"."p_container", ', ') AS "CONTAINER_TYPES"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_name"
HAVING SUM("partsupp"."ps_availqty") > 0