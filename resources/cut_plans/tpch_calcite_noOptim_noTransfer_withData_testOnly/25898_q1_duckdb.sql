SELECT COALESCE("part"."p_type", "part"."p_type") AS "P_TYPE", "supplier"."s_name" AS "S_NAME", "partsupp"."ps_availqty" AS "PS_AVAILQTY", "part"."p_retailprice" AS "P_RETAILPRICE", CONCAT("supplier"."s_name", ': ', "part"."p_name", ' - ', "part"."p_brand", ' [', "part"."p_type", ']') AS "PART_DESCRIPTION", ', ' AS "FD_COL_5"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"