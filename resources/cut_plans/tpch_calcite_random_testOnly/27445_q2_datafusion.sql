SELECT COALESCE("t5"."P_PARTKEY", "t5"."P_PARTKEY") AS "P_PARTKEY", "t5"."SHORT_PART_NAME", "t5"."COMMENT_LENGTH", "t5"."REGION_NAME", "t5"."SUPPLIER_COUNT", "t5"."SUPPLIER_NAMES", "t5"."MAX_EXTENDED_PRICE"
FROM (SELECT "s1"."p_partkey" AS "P_PARTKEY", SUBSTRING("s1"."p_name", 1, 15) AS "$f1", LENGTH("s1"."p_comment") AS "$f2", "region"."r_name", ANY_VALUE(SUBSTRING("s1"."p_name", 1, 15)) AS "SHORT_PART_NAME", ANY_VALUE(LENGTH("s1"."p_comment")) AS "COMMENT_LENGTH", ANY_VALUE("region"."r_name") AS "REGION_NAME", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", LISTAGG(DISTINCT "supplier"."s_name", ', ') AS "SUPPLIER_NAMES", MAX("t1"."l_extendedprice") AS "MAX_EXTENDED_PRICE", COUNT(DISTINCT "t1"."l_orderkey") AS "$f10"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" > (DATE '1998-10-01' - INTERVAL '1' YEAR)) AS "t1" INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."l_partkey" = "s1"."p_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."p_partkey", SUBSTRING("s1"."p_name", 1, 15), LENGTH("s1"."p_comment"), "region"."r_name"
HAVING COUNT(DISTINCT "t1"."l_orderkey") > 5
ORDER BY 6 DESC NULLS FIRST, 8 DESC NULLS FIRST) AS "t5"