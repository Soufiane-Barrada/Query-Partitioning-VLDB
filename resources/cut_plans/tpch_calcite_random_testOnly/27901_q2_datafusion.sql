SELECT COALESCE("t6"."P_BRAND", "t6"."P_BRAND") AS "P_BRAND", "t6"."SUPPLIER_COUNT", "t6"."AVG_PRICE", "t6"."LAST_SHIP_DATE", "t6"."FIRST_SHIP_DATE", "t6"."SUPPLIER_INFO"
FROM (SELECT "s1"."p_brand" AS "P_BRAND", COUNT(DISTINCT "t2"."s_name") AS "SUPPLIER_COUNT", AVG("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "AVG_PRICE", MAX("s1"."l_shipdate") AS "LAST_SHIP_DATE", MIN("s1"."l_shipdate") AS "FIRST_SHIP_DATE", LISTAGG(DISTINCT CONCAT("t2"."s_name", ' (', "t2"."s_phone", ')'), ', ') AS "SUPPLIER_INFO", COUNT(DISTINCT "s1"."l_orderkey") AS "$f6"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 10000.00) AS "t2"
INNER JOIN "s1" ON "t2"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_brand"
HAVING COUNT(DISTINCT "s1"."l_orderkey") > 5
ORDER BY 3 DESC NULLS FIRST) AS "t6"