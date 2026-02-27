SELECT COALESCE("t"."p_partkey", "t"."p_partkey") AS "p_partkey", "t"."p_name", "t"."p_mfgr", "t"."p_brand", "t"."p_type", "t"."p_size", "t"."p_container", "t"."p_retailprice", "t"."p_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment", "t0"."l_orderkey", "t0"."l_partkey", "t0"."l_suppkey", "t0"."l_linenumber", "t0"."l_quantity", "t0"."l_extendedprice", "t0"."l_discount", "t0"."l_tax", "t0"."l_returnflag", "t0"."l_linestatus", "t0"."l_shipdate", "t0"."l_commitdate", "t0"."l_receiptdate", "t0"."l_shipinstruct", "t0"."l_shipmode", "t0"."l_comment"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%metal%') AS "t"
INNER JOIN ("TPCH"."partsupp" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t0" ON "partsupp"."ps_partkey" = "t0"."l_partkey") ON "t"."p_partkey" = "partsupp"."ps_partkey"