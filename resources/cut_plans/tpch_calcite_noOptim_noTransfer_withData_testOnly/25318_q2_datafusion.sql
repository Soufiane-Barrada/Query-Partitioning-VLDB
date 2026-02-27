SELECT COALESCE("SUPPLIER_REGION_NAME", "SUPPLIER_REGION_NAME") AS "SUPPLIER_REGION_NAME", "TOTAL_REVENUE", "ORDERS_COUNT", "AVG_PRICE_BUILDING", "PART_NAMES"
FROM (SELECT ANY_VALUE(CONCAT("s_name", ' from ', "n_name", ' region, ', "r_name")) AS "SUPPLIER_REGION_NAME", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "o_orderkey") AS "ORDERS_COUNT", AVG(CASE WHEN "c_mktsegment" = 'BUILDING' THEN CAST("l_extendedprice" AS DECIMAL(15, 2)) ELSE NULL END) AS "AVG_PRICE_BUILDING", LISTAGG(DISTINCT "p_name", ', ') AS "PART_NAMES"
FROM "s1"
WHERE "r_name" LIKE '%East%' AND "l_shipdate" >= '1996-01-01' AND "l_shipdate" <= '1996-12-31'
GROUP BY CONCAT("s_name", ' from ', "n_name", ' region, ', "r_name")
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"