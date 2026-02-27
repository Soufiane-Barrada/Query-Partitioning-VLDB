SELECT COALESCE("t"."p_partkey", "t"."p_partkey") AS "p_partkey", "t"."p_name", "t"."p_mfgr", "t"."p_brand", "t"."p_type", "t"."p_size", "t"."p_container", "t"."p_retailprice", "t"."p_comment", "supplier"."s_suppkey", "supplier"."s_name", "supplier"."s_address", "supplier"."s_nationkey", "supplier"."s_phone", "supplier"."s_acctbal", "supplier"."s_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_retailprice" >= 10.00 AND "p_retailprice" <= 100.00) AS "t"
INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t"."p_partkey" = "partsupp"."ps_partkey"