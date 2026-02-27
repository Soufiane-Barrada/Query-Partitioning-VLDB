SELECT COALESCE("SUPPLIER_INFO", "SUPPLIER_INFO") AS "SUPPLIER_INFO", "TOTAL_ORDERS", "TOTAL_REVENUE", "AVG_PART_PRICE", "PART_NAMES"
FROM (SELECT ANY_VALUE(CONCAT("s_name", ' from ', "r_name")) AS "SUPPLIER_INFO", COUNT(DISTINCT "o_orderkey") AS "TOTAL_ORDERS", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE", AVG("p_retailprice") AS "AVG_PART_PRICE", LISTAGG(DISTINCT "p_name", ', ') AS "PART_NAMES"
FROM "s1"
WHERE "r_name" LIKE 'Europe%' AND "l_shipdate" >= '1997-01-01' AND "l_shipdate" <= '1997-12-31'
GROUP BY "s_name", "r_name"
HAVING SUM("l_extendedprice" * (1 - "l_discount")) > 100000.0000
ORDER BY 3 DESC NULLS FIRST) AS "t5"