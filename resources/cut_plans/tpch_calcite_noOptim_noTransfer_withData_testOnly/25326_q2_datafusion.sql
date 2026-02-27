SELECT COALESCE("SUPPLIER_NAME", "SUPPLIER_NAME") AS "SUPPLIER_NAME", "PART_NAME", "TOTAL_ORDERS", "TOTAL_REVENUE", "MARKET_SEGMENTS", "FIRST_SHIP_DATE", "LAST_SHIP_DATE"
FROM (SELECT ANY_VALUE("s_name") AS "SUPPLIER_NAME", ANY_VALUE("p_name") AS "PART_NAME", COUNT(DISTINCT "o_orderkey") AS "TOTAL_ORDERS", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE", LISTAGG(DISTINCT "c_mktsegment", ', ') AS "MARKET_SEGMENTS", MIN("l_shipdate") AS "FIRST_SHIP_DATE", MAX("l_shipdate") AS "LAST_SHIP_DATE"
FROM "s1"
WHERE "s_name" LIKE '%Supplier%' AND "c_address" LIKE '%Street%' AND "o_orderstatus" = 'O' AND "l_shipdate" >= '1997-01-01' AND "l_shipdate" <= '1997-12-31'
GROUP BY "s_name", "p_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"