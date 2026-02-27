SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "P_PARTKEY", "part"."p_name" AS "P_NAME", "part"."p_retailprice" AS "P_RETAILPRICE", ANY_VALUE(LENGTH("part"."p_comment")) AS "COMMENT_LENGTH", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QTY", COUNT(DISTINCT "partsupp"."ps_suppkey") AS "SUPPLIER_COUNT", COUNT(DISTINCT "partsupp"."ps_suppkey") > 0 AS "FD_COL_6"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "part"."p_partkey", "part"."p_name", "part"."p_retailprice", LENGTH("part"."p_comment")
HAVING COUNT(DISTINCT "partsupp"."ps_suppkey") > 0