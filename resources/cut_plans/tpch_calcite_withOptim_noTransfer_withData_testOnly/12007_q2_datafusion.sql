SELECT COALESCE("t2"."p_brand", "t2"."p_brand") AS "p_brand", "t2"."p_type", "t2"."TOTAL_QUANTITY", "t2"."AVG_PRICE"
FROM (SELECT "s1"."p_brand", "s1"."p_type", SUM("s1"."l_quantity") AS "TOTAL_QUANTITY", AVG("s1"."l_extendedprice") AS "AVG_PRICE"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_brand", "s1"."p_type"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t2"