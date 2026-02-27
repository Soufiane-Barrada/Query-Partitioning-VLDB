SELECT COALESCE("ps_partkey", "ps_partkey") AS "PS_PARTKEY", SUM("ps_availqty") AS "TOTAL_AVAILABLE_QUANTITY", AVG("ps_supplycost") AS "AVG_SUPPLY_COST"
FROM "TPCH"."partsupp"
GROUP BY "ps_partkey"