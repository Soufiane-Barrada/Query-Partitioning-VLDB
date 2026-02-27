SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "P_NAME", "t0"."s_name" AS "S_NAME", SUM("t0"."ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", COUNT(DISTINCT "t0"."o_orderkey") AS "TOTAL_ORDERS", AVG("t0"."o_totalprice") AS "AVG_ORDER_PRICE", ANY_VALUE(CONCAT("region"."r_name", ' - ', "nation"."n_name")) AS "REGION_NATION"
FROM (SELECT "t"."p_partkey", "t"."p_name", "t"."p_mfgr", "t"."p_brand", "t"."p_type", "t"."p_size", "t"."p_container", "t"."p_retailprice", "t"."p_comment", "t"."ps_partkey", "t"."ps_suppkey", "t"."ps_availqty", "t"."ps_supplycost", "t"."ps_comment", "t"."s_suppkey", "t"."s_name", "t"."s_address", "t"."s_nationkey", "t"."s_phone", "t"."s_acctbal", "t"."s_comment", "orders"."o_orderkey", "orders"."o_custkey", "orders"."o_orderstatus", "orders"."o_totalprice", "orders"."o_orderdate", "orders"."o_orderpriority", "orders"."o_clerk", "orders"."o_shippriority", "orders"."o_comment"
FROM (SELECT "part"."p_partkey", "part"."p_name", "part"."p_mfgr", "part"."p_brand", "part"."p_type", "part"."p_size", "part"."p_container", "part"."p_retailprice", "part"."p_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment", CAST("supplier"."s_nationkey" AS BIGINT) AS "s_nationkey0"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey") AS "t"
INNER JOIN "TPCH"."orders" ON "t"."s_nationkey0" = "orders"."o_custkey") AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "t0"."p_comment" LIKE 'Special%' AND "t0"."o_orderdate" >= '1996-01-01' AND "t0"."o_orderdate" <= '1996-12-31' AND "t0"."s_acctbal" > 500.00
GROUP BY "t0"."p_name", "t0"."s_name", "region"."r_name", "nation"."n_name"
HAVING SUM("t0"."ps_availqty") > 1000