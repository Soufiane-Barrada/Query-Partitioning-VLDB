SELECT COALESCE("partsupp"."ps_suppkey", "partsupp"."ps_suppkey") AS "PS_SUPPKEY", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_SUPPLY_VALUE", COUNT(DISTINCT "part"."p_partkey") AS "TOTAL_PARTS_SUPPLIED", MOD("partsupp"."ps_suppkey", 10) AS "FD_COL_3"
FROM "TPCH"."part"
INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "partsupp"."ps_suppkey"