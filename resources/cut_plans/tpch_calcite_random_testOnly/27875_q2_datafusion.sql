SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."S_NAME", "t4"."CUSTOMER_COUNT", "t4"."TOTAL_REVENUE", "t4"."AVG_DISCOUNT", "t4"."FIRST_SHIP_DATE", "t4"."LAST_SHIP_DATE", "t4"."SUPPLIER_ADDRESSES"
FROM (SELECT "t1"."p_name" AS "P_NAME", "supplier"."s_name" AS "S_NAME", COUNT(DISTINCT "s1"."c_custkey") AS "CUSTOMER_COUNT", SUM("s1"."l_extendedprice") AS "TOTAL_REVENUE", AVG("s1"."l_discount") AS "AVG_DISCOUNT", MIN("s1"."l_shipdate") AS "FIRST_SHIP_DATE", MAX("s1"."l_shipdate") AS "LAST_SHIP_DATE", LISTAGG(DISTINCT CONCAT("supplier"."s_name", ' - ', "supplier"."s_address"), '; ') AS "SUPPLIER_ADDRESSES"
FROM "TPCH"."supplier"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%Steel%') AS "t1" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "s1" ON "t1"."p_partkey" = "s1"."l_partkey"
GROUP BY "t1"."p_name", "supplier"."s_name"
ORDER BY 4 DESC NULLS FIRST, 3 DESC NULLS FIRST) AS "t4"