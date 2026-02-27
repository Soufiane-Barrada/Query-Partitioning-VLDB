SELECT COALESCE("p_brand", "p_brand") AS "p_brand", "p_type", "TOTAL_AVAIL_QTY", "AVG_SUPPLY_COST"
FROM (SELECT "p_brand", "p_type", SUM("ps_availqty") AS "TOTAL_AVAIL_QTY", AVG("ps_supplycost") AS "AVG_SUPPLY_COST"
FROM "s1"
GROUP BY "p_brand", "p_type"
ORDER BY 3 DESC NULLS FIRST, 4
FETCH NEXT 100 ROWS ONLY) AS "t1"