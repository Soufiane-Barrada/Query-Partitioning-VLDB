SELECT COALESCE("t4"."SUPPLIER_NAME", "t4"."SUPPLIER_NAME") AS "SUPPLIER_NAME", "t4"."PART_NAME", "t4"."TOTAL_ORDERS", "t4"."TOTAL_REVENUE", "t4"."AVG_RETURN_AMT", "t4"."SUPPLIER_COMMENTS", "t4"."SANITIZED_PART_NAME"
FROM (SELECT "supplier"."s_name", "part"."p_name", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("part"."p_name") AS "PART_NAME", COUNT(DISTINCT "s1"."o_orderkey") AS "TOTAL_ORDERS", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "TOTAL_REVENUE", AVG(CASE WHEN "s1"."l_returnflag" = 'R' THEN CAST("s1"."l_quantity" AS DECIMAL(15, 2)) ELSE NULL END) AS "AVG_RETURN_AMT", LISTAGG(DISTINCT CONCAT("nation"."n_name", ': ', "supplier"."s_comment"), '; ') AS "SUPPLIER_COMMENTS", ANY_VALUE(REGEXP_REPLACE(UPPER("part"."p_name"), '[^A-Z0-9 ]', '')) AS "SANITIZED_PART_NAME", AVG("s1"."l_quantity") AS "$f9"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "s1" ON "part"."p_partkey" = "s1"."l_partkey"
GROUP BY "supplier"."s_name", "part"."p_name"
HAVING AVG("s1"."l_quantity") > 5.00
ORDER BY 6 DESC NULLS FIRST) AS "t4"