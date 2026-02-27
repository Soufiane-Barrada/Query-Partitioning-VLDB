SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "S_NAME", "N_NAME", "R_NAME", "TOTAL_ORDERS", "TOTAL_REVENUE", "AVG_SUPPLIER_BALANCE", "ORDER_PRIORITIES"
FROM (SELECT "p_name" AS "P_NAME", "s_name" AS "S_NAME", "n_name" AS "N_NAME", "r_name" AS "R_NAME", COUNT(DISTINCT "o_orderkey") AS "TOTAL_ORDERS", SUM("l_extendedprice") AS "TOTAL_REVENUE", AVG("s_acctbal") AS "AVG_SUPPLIER_BALANCE", LISTAGG(DISTINCT "o_orderpriority", ', ') AS "ORDER_PRIORITIES"
FROM "s1"
WHERE "p_name" LIKE '%widget%' AND "o_orderstatus" = 'O' AND "l_shipdate" >= '1997-01-01'
GROUP BY "p_name", "s_name", "n_name", "r_name"
HAVING SUM("l_extendedprice") > 10000.00
ORDER BY 6 DESC NULLS FIRST, 5 DESC NULLS FIRST) AS "t4"