SELECT COALESCE("p_partkey", "p_partkey") AS "P_PARTKEY", "p_name" AS "P_NAME", "p_brand" AS "P_BRAND", "p_retailprice" AS "P_RETAILPRICE", LENGTH(REPLACE(LOWER("p_comment"), ' ', '')) AS "NON_SPACE_COMMENT_LENGTH"
FROM "TPCH"."part"
WHERE "p_retailprice" > 100.00