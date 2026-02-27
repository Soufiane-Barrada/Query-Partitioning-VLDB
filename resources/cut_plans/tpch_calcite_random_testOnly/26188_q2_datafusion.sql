SELECT COALESCE("t4"."SUPPLIER_NAME", "t4"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t4"."TOTAL_AVAILABLE_QTY", "t4"."UNIQUE_PARTS_SUPPLIED", "t4"."SUPPLIED_PART_DETAILS", "t4"."NATION_NAME", "t4"."DETAILED_INFO"
FROM (SELECT "s1"."s_suppkey", "s1"."s_name", "s1"."n_name", "s1"."r_name", "s1"."s_address", ANY_VALUE("s1"."s_name") AS "SUPPLIER_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QTY", COUNT(DISTINCT "t1"."p_partkey") AS "UNIQUE_PARTS_SUPPLIED", LISTAGG(DISTINCT CONCAT("t1"."p_name", ' (', "t1"."p_brand", ')'), ', ') AS "SUPPLIED_PART_DETAILS", ANY_VALUE("s1"."n_name") AS "NATION_NAME", ANY_VALUE(CONCAT('Region: ', "s1"."r_name", ', Supplier Address: ', "s1"."s_address")) AS "DETAILED_INFO"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 50) AS "t1"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "s1"."s_suppkey", "s1"."s_name", "s1"."n_name", "s1"."r_name", "s1"."s_address"
ORDER BY 7 DESC NULLS FIRST, 8 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"