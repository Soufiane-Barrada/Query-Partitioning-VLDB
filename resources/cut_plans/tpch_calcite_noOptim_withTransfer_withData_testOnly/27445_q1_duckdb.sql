SELECT COALESCE("part"."p_partkey", "part"."p_partkey") AS "P_PARTKEY", ANY_VALUE(SUBSTRING("part"."p_name", 1, 15)) AS "SHORT_PART_NAME", ANY_VALUE(LENGTH("part"."p_comment")) AS "COMMENT_LENGTH", ANY_VALUE("region"."r_name") AS "REGION_NAME", COUNT(DISTINCT "supplier"."s_suppkey") AS "SUPPLIER_COUNT", LISTAGG(DISTINCT "supplier"."s_name", ', ') AS "SUPPLIER_NAMES", MAX("lineitem"."l_extendedprice") AS "MAX_EXTENDED_PRICE"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey"
WHERE "part"."p_size" >= 10 AND "part"."p_size" <= 20 AND "lineitem"."l_shipdate" > (DATE '1998-10-01' - INTERVAL '1' YEAR)
GROUP BY "part"."p_partkey", SUBSTRING("part"."p_name", 1, 15), LENGTH("part"."p_comment"), "region"."r_name"
HAVING COUNT(DISTINCT "lineitem"."l_orderkey") > 5