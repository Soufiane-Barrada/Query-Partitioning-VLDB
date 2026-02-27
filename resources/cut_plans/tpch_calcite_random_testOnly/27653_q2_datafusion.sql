SELECT COALESCE("t7"."P_NAME", "t7"."P_NAME") AS "P_NAME", "t7"."S_NAME", "t7"."TOTAL_AVAILABLE_QUANTITY", "t7"."TOTAL_ORDERS", "t7"."AVG_ORDER_PRICE", "t7"."REGION_NATION"
FROM (SELECT "t3"."p_name" AS "P_NAME", "t3"."s_name" AS "S_NAME", "region"."r_name", "nation"."n_name", SUM("t3"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", COUNT(DISTINCT "t3"."o_orderkey") AS "TOTAL_ORDERS", AVG("t3"."o_totalprice") AS "AVG_ORDER_PRICE", ANY_VALUE(CONCAT("region"."r_name", ' - ', "nation"."n_name")) AS "REGION_NATION"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT "t2"."p_partkey", "t2"."p_name", "t2"."p_mfgr", "t2"."p_brand", "t2"."p_type", "t2"."p_size", "t2"."p_container", "t2"."p_retailprice", "t2"."p_comment", "t2"."ps_partkey", "t2"."ps_suppkey", "t2"."ps_availqty", "t2"."ps_supplycost", "t2"."ps_comment", "t2"."s_suppkey", "t2"."s_name", "t2"."s_address", "t2"."s_nationkey", "t2"."s_phone", "t2"."s_acctbal", "t2"."s_comment", "s1"."o_orderkey", "s1"."o_custkey", "s1"."o_orderstatus", "s1"."o_totalprice", "s1"."o_orderdate", "s1"."o_orderpriority", "s1"."o_clerk", "s1"."o_shippriority", "s1"."o_comment"
FROM (SELECT "part"."p_partkey", "part"."p_name", "part"."p_mfgr", "part"."p_brand", "part"."p_type", "part"."p_size", "part"."p_container", "part"."p_retailprice", "part"."p_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment", CAST("supplier"."s_nationkey" AS BIGINT) AS "s_nationkey0"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
WHERE "part"."p_comment" LIKE 'Special%' AND "supplier"."s_acctbal" > 500.00) AS "t2"
INNER JOIN "s1" ON "t2"."s_nationkey0" = "s1"."o_custkey") AS "t3" ON "nation"."n_nationkey" = "t3"."s_nationkey"
GROUP BY "t3"."p_name", "t3"."s_name", "region"."r_name", "nation"."n_name"
HAVING SUM("t3"."ps_availqty") > 1000
ORDER BY 7 DESC NULLS FIRST, 6 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t7"