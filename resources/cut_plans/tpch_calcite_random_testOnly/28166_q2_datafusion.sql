SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."SUPPLIER_COUNT", "t4"."SUPPLIER_DETAILS", "t4"."TOTAL_AVAILABLE_QUANTITY", "t4"."AVERAGE_SUPPLY_COST", "t4"."MAX_SUPPLY_COST", "t4"."MIN_SUPPLY_COST"
FROM (SELECT "s1"."p_name" AS "P_NAME", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", LISTAGG(CONCAT("supplier"."s_name", ' (', "supplier"."s_phone", ')'), '; ') AS "SUPPLIER_DETAILS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", MAX("partsupp"."ps_supplycost") AS "MAX_SUPPLY_COST", MIN("partsupp"."ps_supplycost") AS "MIN_SUPPLY_COST"
FROM "TPCH"."supplier"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."p_name"
HAVING COUNT(DISTINCT "partsupp"."ps_suppkey") > 5
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"