SELECT COALESCE("t10"."NATION_NAME", "t10"."NATION_NAME") AS "NATION_NAME", "t10"."TOTAL_CUSTOMERS", "t10"."TOTAL_PARTS_AVAILABLE", "t10"."AVG_CUSTOMER_SPENDING"
FROM (SELECT "t5"."N_NAME", ANY_VALUE("t5"."N_NAME") AS "NATION_NAME", COUNT(DISTINCT "t8"."C_CUSTKEY") AS "TOTAL_CUSTOMERS", SUM("t5"."TOTAL_AVAILABLE") AS "TOTAL_PARTS_AVAILABLE", AVG("t8"."CUSTOMER_TOTAL") AS "AVG_CUSTOMER_SPENDING"
FROM (SELECT "t3"."N_NAME", "t3"."N_REGIONKEY", "t3"."SUPPLIER_COUNT", "t4"."ps_suppkey" AS "PS_SUPPKEY", "t4"."TOTAL_AVAILABLE"
FROM (SELECT "nation"."n_name" AS "N_NAME", "nation"."n_regionkey" AS "N_REGIONKEY", COUNT(*) AS "SUPPLIER_COUNT", COUNT(*) > 10 AS "$f3"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
GROUP BY "nation"."n_name", "nation"."n_regionkey"
HAVING COUNT(*) > 10) AS "t3",
(SELECT "ps_suppkey", SUM("ps_availqty") AS "TOTAL_AVAILABLE"
FROM "TPCH"."partsupp"
GROUP BY "ps_suppkey") AS "t4") AS "t5"
INNER JOIN (SELECT "s1"."c_custkey" AS "C_CUSTKEY", "s1"."c_name" AS "C_NAME", "s1"."CUSTOMER_TOTAL", "t7"."$f0"
FROM (SELECT SINGLE_VALUE("r_regionkey") AS "$f0"
FROM "TPCH"."region"
WHERE "r_name" = 'Asia') AS "t7",
"s1") AS "t8" ON "t5"."N_REGIONKEY" = "t8"."$f0"
GROUP BY "t5"."N_NAME"
ORDER BY 3 DESC NULLS FIRST, 5 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t10"