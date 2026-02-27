SELECT COALESCE("P_NAME", "P_NAME") AS "P_NAME", "S_NAME", "SUPPLIER_INFO", "TOTAL_REVENUE"
FROM (SELECT "p_name" AS "P_NAME", "s_name" AS "S_NAME", ANY_VALUE(CONCAT('Region: ', "r_name", ', Nation: ', "n_name", ', Supplier: ', "s_name")) AS "SUPPLIER_INFO", SUM("l_extendedprice" * (1 - "l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
WHERE "p_name" LIKE 'prod_%' AND "s_comment" NOT LIKE '%fraud%' AND "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31'
GROUP BY "p_name", "s_name", "r_name", "n_name"
HAVING SUM("l_extendedprice" * (1 - "l_discount")) > 1000.0000
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t5"