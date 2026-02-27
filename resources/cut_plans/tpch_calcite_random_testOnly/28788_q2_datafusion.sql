SELECT COALESCE("t4"."SUPPLIER_NAME", "t4"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t4"."TOTAL_PARTS_SUPPLIED", "t4"."TOTAL_QUANTITY_AVAILABLE", "t4"."AVERAGE_PART_PRICE", "t4"."TOTAL_RETURNED_QUANTITY", "t4"."NATIONS_SUPPLIED", "t4"."REGIONS_SERVED"
FROM (SELECT "s1"."s_name", ANY_VALUE("s1"."s_name") AS "SUPPLIER_NAME", COUNT(DISTINCT "partsupp"."ps_partkey") AS "TOTAL_PARTS_SUPPLIED", SUM("partsupp"."ps_availqty") AS "TOTAL_QUANTITY_AVAILABLE", AVG("part"."p_retailprice") AS "AVERAGE_PART_PRICE", SUM(CASE WHEN "lineitem"."l_returnflag" = 'R' THEN "lineitem"."l_quantity" ELSE 0.00 END) AS "TOTAL_RETURNED_QUANTITY", LISTAGG(DISTINCT "nation"."n_name", ', ') AS "NATIONS_SUPPLIED", LISTAGG(DISTINCT "t1"."r_name", ', ') AS "REGIONS_SERVED"
FROM "TPCH"."lineitem"
RIGHT JOIN ("TPCH"."part" INNER JOIN ("TPCH"."partsupp" INNER JOIN ("s1" INNER JOIN ("TPCH"."nation" INNER JOIN (SELECT *
FROM "TPCH"."region"
WHERE "r_name" LIKE 'Asia%') AS "t1" ON "nation"."n_regionkey" = "t1"."r_regionkey") ON "s1"."s_nationkey" = "nation"."n_nationkey") ON "partsupp"."ps_suppkey" = "s1"."s_suppkey") ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "s1"."s_suppkey"
GROUP BY "s1"."s_name"
ORDER BY 3 DESC NULLS FIRST, 4 DESC NULLS FIRST) AS "t4"