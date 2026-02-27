SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "P_NAME", "t0"."p_brand" AS "P_BRAND", "t0"."p_partkey", SUBSTRING("t0"."p_comment", 1, 20) AS "FD_COL_3", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", LISTAGG(DISTINCT CONCAT("nation"."n_name", ': ', "nation"."n_comment"), '; ') AS "NATION_COMMENTS", ANY_VALUE(SUBSTRING("t0"."p_comment", 1, 20)) AS "SHORT_COMMENT"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT AVG("p_retailprice") AS "EXPR$0"
FROM "TPCH"."part") AS "t" INNER JOIN (SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 100) AS "t0" ON "t"."EXPR$0" < "t0"."p_retailprice" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_name", "t0"."p_brand", "t0"."p_partkey", SUBSTRING("t0"."p_comment", 1, 20)