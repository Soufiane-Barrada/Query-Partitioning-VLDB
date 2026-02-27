SELECT COALESCE("part"."p_name", "part"."p_name") AS "P_NAME", "part"."p_brand" AS "P_BRAND", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", LISTAGG(DISTINCT CONCAT("nation"."n_name", ': ', "nation"."n_comment"), '; ') AS "NATION_COMMENTS", ANY_VALUE(SUBSTRING("part"."p_comment", 1, 20)) AS "SHORT_COMMENT"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
WHERE "part"."p_size" >= 10 AND "part"."p_size" <= 100 AND "part"."p_retailprice" > (((SELECT AVG("p_retailprice")
FROM "TPCH"."part")))
GROUP BY "part"."p_name", "part"."p_brand", "part"."p_partkey", SUBSTRING("part"."p_comment", 1, 20)