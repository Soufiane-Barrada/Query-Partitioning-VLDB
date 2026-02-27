SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", LISTAGG(CONCAT("supplier"."s_name", ' (', "supplier"."s_phone", ')'), '; ') AS "SUPPLIER_DETAILS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", MAX("partsupp"."ps_supplycost") AS "MAX_SUPPLY_COST", MIN("partsupp"."ps_supplycost") AS "MIN_SUPPLY_COST"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
WHERE "part"."p_mfgr" LIKE '%Manufacturer%'
GROUP BY "part"."p_name"
HAVING COUNT(DISTINCT "partsupp"."ps_suppkey") > 5