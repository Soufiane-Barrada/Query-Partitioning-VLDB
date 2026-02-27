SELECT COALESCE("t5"."SUPPLIER_INFO", "t5"."SUPPLIER_INFO") AS "SUPPLIER_INFO", "t5"."UNIQUE_PARTS", "t5"."TOTAL_AVAILABLE_QUANTITY", "t5"."TOTAL_SUPPLY_COST"
FROM (SELECT "supplier"."s_name", "nation"."n_name", "t1"."r_name", ANY_VALUE(CONCAT("supplier"."s_name", ' from ', "nation"."n_name", ', located in ', "t1"."r_name")) AS "SUPPLIER_INFO", COUNT(DISTINCT "s1"."p_partkey") AS "UNIQUE_PARTS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", SUM("partsupp"."ps_supplycost") AS "TOTAL_SUPPLY_COST"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" IN ('ASIA', 'EUROPE')) AS "t1"
INNER JOIN "TPCH"."nation" ON "t1"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_name", "nation"."n_name", "t1"."r_name"
HAVING COUNT(DISTINCT "s1"."p_partkey") > 5
ORDER BY 7 DESC NULLS FIRST, 5) AS "t5"