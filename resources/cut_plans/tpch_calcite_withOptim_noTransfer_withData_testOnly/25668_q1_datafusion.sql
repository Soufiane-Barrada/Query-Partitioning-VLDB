SELECT COALESCE("p_partkey", "p_partkey") AS "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"
FROM "TPCH"."part"
WHERE "p_size" >= 1 AND "p_size" <= 50 AND "p_comment" LIKE '%excellent%'