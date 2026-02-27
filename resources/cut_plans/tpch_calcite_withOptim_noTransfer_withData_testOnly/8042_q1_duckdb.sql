SELECT COALESCE("t1"."s_suppkey", "t1"."s_suppkey") AS "S_SUPPKEY", "t1"."s_name" AS "S_NAME", "t1"."ORDER_COUNT", "t1"."TOTAL_REVENUE", "region"."r_regionkey", "region"."r_name", "region"."r_comment", "nation"."n_nationkey", "nation"."n_name", "nation"."n_regionkey", "nation"."n_comment", "supplier0"."s_suppkey" AS "s_suppkey_", "supplier0"."s_name" AS "s_name_", "supplier0"."s_address", "supplier0"."s_nationkey", "supplier0"."s_phone", "supplier0"."s_acctbal", "supplier0"."s_comment", "t2"."p_partkey", "t2"."p_name", "t2"."p_mfgr", "t2"."p_brand", "t2"."p_type", "t2"."p_size", "t2"."p_container", "t2"."p_retailprice", "t2"."p_comment", "partsupp0"."ps_partkey", "partsupp0"."ps_suppkey", "partsupp0"."ps_availqty", "partsupp0"."ps_supplycost", "partsupp0"."ps_comment"
FROM (SELECT "supplier"."s_suppkey", "supplier"."s_name", COUNT(*) AS "ORDER_COUNT", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t" INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
GROUP BY "supplier"."s_suppkey", "supplier"."s_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t1"
INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" AS "supplier0" ON "nation"."n_nationkey" = "supplier0"."s_nationkey") ON "t1"."s_suppkey" = "supplier0"."s_suppkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00) AS "t2" INNER JOIN "TPCH"."partsupp" AS "partsupp0" ON "t2"."p_partkey" = "partsupp0"."ps_partkey") ON "t1"."s_suppkey" = "partsupp0"."ps_suppkey"