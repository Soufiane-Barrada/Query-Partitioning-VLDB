SELECT COALESCE("p_partkey", "p_partkey") AS "P_PARTKEY", LENGTH("p_name") AS "NAME_LENGTH", LENGTH("p_comment") AS "COMMENT_LENGTH", 'Part Name: ' || "p_name" || ' | Comment: ' || "p_comment" AS "CONCATENATED_STRING", '; ' AS "FD_COL_4"
FROM "TPCH"."part"
WHERE "p_size" > 0