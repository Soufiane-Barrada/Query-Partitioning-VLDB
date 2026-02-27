SELECT COALESCE("t0"."p_partkey", "t0"."p_partkey") AS "P_PARTKEY", SUBSTRING("t0"."p_name", 1, 15) AS "FD_COL_1", LENGTH("t0"."p_comment") AS "FD_COL_2", "region"."r_name", ANY_VALUE(SUBSTRING("t0"."p_name", 1, 15)) AS "SHORT_PART_NAME", ANY_VALUE(LENGTH("t0"."p_comment")) AS "COMMENT_LENGTH", ANY_VALUE("region"."r_name") AS "REGION_NAME", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", LISTAGG(DISTINCT "supplier"."s_name", ', ') AS "SUPPLIER_NAMES", MAX("t"."l_extendedprice") AS "MAX_EXTENDED_PRICE", COUNT(DISTINCT "t"."l_orderkey") AS "FD_COL_10"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" > (DATE '1998-10-01' - INTERVAL '1' YEAR)) AS "t" INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" >= 10 AND "p_size" <= 20) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."l_partkey" = "t0"."p_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_partkey", SUBSTRING("t0"."p_name", 1, 15), LENGTH("t0"."p_comment"), "region"."r_name"
HAVING COUNT(DISTINCT "t"."l_orderkey") > 5