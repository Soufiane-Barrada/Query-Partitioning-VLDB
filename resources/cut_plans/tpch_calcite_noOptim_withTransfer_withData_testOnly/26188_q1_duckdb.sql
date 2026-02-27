SELECT COALESCE(ANY_VALUE("supplier"."s_name"), ANY_VALUE("supplier"."s_name")) AS "SUPPLIER_NAME", SUM("partsupp"."ps_availqty") AS "TOTAL_AVAILABLE_QTY", COUNT(DISTINCT "part"."p_partkey") AS "UNIQUE_PARTS_SUPPLIED", LISTAGG(DISTINCT CONCAT("part"."p_name", ' (', "part"."p_brand", ')'), ', ') AS "SUPPLIED_PART_DETAILS", ANY_VALUE("nation"."n_name") AS "NATION_NAME", ANY_VALUE(CONCAT('Region: ', "region"."r_name", ', Supplier Address: ', "supplier"."s_address")) AS "DETAILED_INFO"
FROM "TPCH"."supplier"
INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."part" ON "partsupp"."ps_partkey" = "part"."p_partkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "part"."p_size" > 50 AND "supplier"."s_acctbal" > 1000.00
GROUP BY "supplier"."s_suppkey", "supplier"."s_name", "nation"."n_name", "region"."r_name", "supplier"."s_address"