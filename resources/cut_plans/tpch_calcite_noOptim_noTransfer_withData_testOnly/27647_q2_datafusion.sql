SELECT COALESCE("SUPPLIER_REGION", "SUPPLIER_REGION") AS "SUPPLIER_REGION", "TOTAL_QUANTITY", "AVG_PRICE", "ORDER_COUNT", "PART_NAMES"
FROM (SELECT ANY_VALUE(CONCAT("s_name", ' (', "r_name", ')')) AS "SUPPLIER_REGION", SUM("l_quantity") AS "TOTAL_QUANTITY", AVG("l_extendedprice") AS "AVG_PRICE", COUNT(DISTINCT "o_orderkey") AS "ORDER_COUNT", LISTAGG(DISTINCT "p_name", ', ') AS "PART_NAMES"
FROM "s1"
WHERE "l_shipdate" >= '1997-01-01' AND "l_shipdate" <= '1997-12-31' AND "o_orderstatus" = 'O'
GROUP BY CONCAT("s_name", ' (', "r_name", ')')
HAVING SUM("l_quantity") > 100.00
ORDER BY 2 DESC NULLS FIRST) AS "t5"