SELECT COALESCE("P_BRAND", "P_BRAND") AS "P_BRAND", "SUPPLIER_COUNT", "AVG_PRICE", "LAST_SHIP_DATE", "FIRST_SHIP_DATE", "SUPPLIER_INFO"
FROM (SELECT "p_brand" AS "P_BRAND", COUNT(DISTINCT "s_name") AS "SUPPLIER_COUNT", AVG("l_extendedprice" * (1 - "l_discount")) AS "AVG_PRICE", MAX("l_shipdate") AS "LAST_SHIP_DATE", MIN("l_shipdate") AS "FIRST_SHIP_DATE", LISTAGG(DISTINCT CONCAT("s_name", ' (', "s_phone", ')'), ', ') AS "SUPPLIER_INFO"
FROM "s1"
WHERE "p_type" LIKE '%metal%' AND "s_acctbal" > 10000.00 AND "l_shipdate" >= '1997-01-01' AND "l_shipdate" <= '1997-12-31'
GROUP BY "p_brand"
HAVING COUNT(DISTINCT "l_orderkey") > 5
ORDER BY 3 DESC NULLS FIRST) AS "t5"