SELECT COALESCE("p_partkey", "p_partkey") AS "P_PARTKEY", "p_name" AS "P_NAME", "p_mfgr" AS "P_MFGR", "p_brand" AS "P_BRAND", "p_type" AS "P_TYPE", "p_size" AS "P_SIZE", "p_container" AS "P_CONTAINER", "p_retailprice" AS "P_RETAILPRICE", "p_comment" AS "P_COMMENT", ROW_NUMBER() OVER (PARTITION BY "p_brand" ORDER BY "p_retailprice" DESC NULLS FIRST) AS "PRICE_RANK"
FROM "TPCH"."part"
WHERE "p_size" = (((SELECT MAX("p_size")
FROM "TPCH"."part")))