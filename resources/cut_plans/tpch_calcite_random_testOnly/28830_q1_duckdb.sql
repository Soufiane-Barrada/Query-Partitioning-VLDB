SELECT COALESCE("t"."p_partkey", "t"."p_partkey") AS "p_partkey", "t"."p_name", "t"."p_mfgr", "t"."p_brand", "t"."p_type", "t"."p_size", "t"."p_container", "t"."p_retailprice", "t"."p_comment", "partsupp"."ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_brand" LIKE 'Brand%') AS "t"
INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey"