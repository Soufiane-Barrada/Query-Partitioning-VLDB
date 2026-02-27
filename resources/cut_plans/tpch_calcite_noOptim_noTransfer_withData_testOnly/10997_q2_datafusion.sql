SELECT COALESCE("L_ORDERKEY", "L_ORDERKEY") AS "L_ORDERKEY", "REVENUE", "O_ORDERDATE", "C_NAME", "S_NAME", "P_NAME", "N_NAME", "R_NAME"
FROM (SELECT "l_orderkey" AS "L_ORDERKEY", SUM("l_extendedprice" * (1 - "l_discount")) AS "REVENUE", "o_orderdate" AS "O_ORDERDATE", "c_name" AS "C_NAME", "s_name" AS "S_NAME", "p_name" AS "P_NAME", "n_name" AS "N_NAME", "r_name" AS "R_NAME"
FROM "s1"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31'
GROUP BY "l_orderkey", "o_orderdate", "c_name", "s_name", "p_name", "n_name", "r_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"