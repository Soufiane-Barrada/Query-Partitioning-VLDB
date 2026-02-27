SELECT COALESCE("t2"."s_name", "t2"."s_name") AS "s_name", "t2"."p_name", "t2"."ps_comment", CONCAT('Supplier: ', "t2"."s_name", ' | Product: ', "t2"."p_name", ' | Comment: ', "t2"."ps_comment") AS "FD_COL_3", LENGTH("t2"."ps_comment") AS "FD_COL_4", SUBSTRING("t2"."ps_comment", CASE WHEN 1 + (LENGTH("t2"."ps_comment") + 1) < 1 THEN LENGTH("t2"."ps_comment") + 1 ELSE 1 END, 20) AS "FD_COL_5", CAST("t"."o_orderkey" AS BIGINT) AS "o_orderkey"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderstatus" = 'O') AS "t"
INNER JOIN (SELECT "t1"."s_suppkey", "t1"."s_name", "t1"."s_address", "t1"."s_nationkey", "t1"."s_phone", "t1"."s_acctbal", "t1"."s_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment", "t0"."p_partkey", "t0"."p_name", "t0"."p_mfgr", "t0"."p_brand", "t0"."p_type", "t0"."p_size", "t0"."p_container", "t0"."p_retailprice", "t0"."p_comment", "lineitem"."l_orderkey", "lineitem"."l_partkey", "lineitem"."l_suppkey", "lineitem"."l_linenumber", "lineitem"."l_quantity", "lineitem"."l_extendedprice", "lineitem"."l_discount", "lineitem"."l_tax", "lineitem"."l_returnflag", "lineitem"."l_linestatus", "lineitem"."l_shipdate", "lineitem"."l_commitdate", "lineitem"."l_receiptdate", "lineitem"."l_shipinstruct", "lineitem"."l_shipmode", "lineitem"."l_comment"
FROM "TPCH"."lineitem"
RIGHT JOIN ("TPCH"."partsupp" INNER JOIN (SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE 'METAL%') AS "t0" ON "partsupp"."ps_partkey" = "t0"."p_partkey" INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 1000.00) AS "t1" ON "partsupp"."ps_suppkey" = "t1"."s_suppkey") ON "lineitem"."l_partkey" = "t0"."p_partkey") AS "t2" ON "t"."o_orderkey" = "t2"."l_orderkey"