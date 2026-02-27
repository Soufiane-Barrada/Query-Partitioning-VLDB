SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."P_BRAND", "t4"."SUPPLIER_COUNT", "t4"."TOTAL_AVAILABLE_QUANTITY", "t4"."AVERAGE_SUPPLY_COST", "t4"."NATION_COMMENTS", "t4"."SHORT_COMMENT"
FROM (SELECT "t1"."p_name" AS "P_NAME", "t1"."p_brand" AS "P_BRAND", "t1"."p_partkey", SUBSTRING("t1"."p_comment", 1, 20) AS "$f3", COUNT(DISTINCT "s1"."s_suppkey") AS "SUPPLIER_COUNT", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", LISTAGG(DISTINCT CONCAT("s1"."n_name", ': ', "s1"."n_comment"), '; ') AS "NATION_COMMENTS", ANY_VALUE(SUBSTRING("t1"."p_comment", 1, 20)) AS "SHORT_COMMENT"
FROM "s1"
INNER JOIN ((SELECT AVG("p_retailprice") AS "EXPR$0"
FROM "TPCH"."part") AS "t0" INNER JOIN (SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 100) AS "t1" ON "t0"."EXPR$0" < "t1"."p_retailprice" INNER JOIN "TPCH"."partsupp" ON "t1"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t1"."p_name", "t1"."p_brand", "t1"."p_partkey", SUBSTRING("t1"."p_comment", 1, 20)
ORDER BY 6 DESC NULLS FIRST, 7
FETCH NEXT 50 ROWS ONLY) AS "t4"