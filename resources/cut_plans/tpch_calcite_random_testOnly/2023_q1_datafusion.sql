SELECT COALESCE("partsupp"."ps_suppkey", "partsupp"."ps_suppkey") AS "PS_SUPPKEY", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_SUPPLY_COST"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "partsupp"."ps_suppkey"
HAVING SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") > 10000000.00