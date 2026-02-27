SELECT COALESCE("partsupp"."ps_partkey", "partsupp"."ps_partkey") AS "ps_partkey", "partsupp"."ps_suppkey", "partsupp"."ps_availqty", "partsupp"."ps_supplycost", "partsupp"."ps_comment", "t"."s_suppkey", "t"."s_name", "t"."s_address", "t"."s_nationkey", "t"."s_phone", "t"."s_acctbal", "t"."s_comment"
FROM "TPCH"."partsupp"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_comment" LIKE '%urgent%') AS "t" ON "partsupp"."ps_suppkey" = "t"."s_suppkey"