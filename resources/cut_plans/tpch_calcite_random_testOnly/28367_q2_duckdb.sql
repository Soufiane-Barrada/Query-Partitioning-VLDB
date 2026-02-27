SELECT COALESCE("t5"."P_BRAND", "t5"."P_BRAND") AS "P_BRAND", "t5"."SUPPLIER_COUNT", "t5"."AVG_SUPPLY_COST", "t5"."FIRST_SHIP_DATE", "t5"."LAST_SHIP_DATE", "t5"."PART_NAMES", "t5"."TOTAL_REVENUE"
FROM (SELECT "t1"."p_brand" AS "P_BRAND", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", AVG("partsupp"."ps_supplycost") AS "AVG_SUPPLY_COST", MIN("t2"."l_shipdate") AS "FIRST_SHIP_DATE", MAX("t2"."l_shipdate") AS "LAST_SHIP_DATE", LISTAGG(DISTINCT TRIM(BOTH ' ' FROM "t1"."p_name"), ', ') AS "PART_NAMES", SUM("t2"."l_extendedprice" * (1 - "t2"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 20) AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("s1" INNER JOIN ("TPCH"."orders" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipmode" IN ('AIR', 'RAIL')) AS "t2" ON "orders"."o_orderkey" = "t2"."l_orderkey") ON "s1"."c_custkey" = "orders"."o_custkey") ON "t1"."p_partkey" = "t2"."l_partkey"
GROUP BY "t1"."p_brand"
ORDER BY 7 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5"