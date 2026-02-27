SELECT COALESCE("t"."p_name", "t"."p_name") AS "P_NAME", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", LISTAGG(CONCAT("supplier"."s_name", ' (', "supplier"."s_phone", ')'), '; ') AS "SUPPLIER_DETAILS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", MAX("partsupp"."ps_supplycost") AS "MAX_SUPPLY_COST", MIN("partsupp"."ps_supplycost") AS "MIN_SUPPLY_COST"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_mfgr" LIKE '%Manufacturer%') AS "t" INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t"."p_name"