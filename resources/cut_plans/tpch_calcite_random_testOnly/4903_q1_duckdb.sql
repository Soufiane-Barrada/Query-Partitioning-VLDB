SELECT COALESCE("ps_partkey", "ps_partkey") AS "ps_partkey", COUNT(DISTINCT "ps_suppkey") AS "SUPPLIER_COUNT"
FROM "TPCH"."partsupp"
GROUP BY "ps_partkey"