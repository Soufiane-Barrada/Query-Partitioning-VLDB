SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "s_name", "nation"."n_name", "t"."r_name", ANY_VALUE(CONCAT("supplier"."s_name", ' from ', "nation"."n_name", ', located in ', "t"."r_name")) AS "SUPPLIER_INFO", COUNT(DISTINCT "t0"."p_partkey") AS "UNIQUE_PARTS", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", SUM("partsupp"."ps_supplycost") AS "TOTAL_SUPPLY_COST"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" IN ('ASIA', 'EUROPE')) AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_brand" LIKE '%BRAND%' AND "p_comment" NOT LIKE '%defective%') AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_name", "nation"."n_name", "t"."r_name"
HAVING COUNT(DISTINCT "t0"."p_partkey") > 5