SELECT COALESCE("supplier"."s_name", "supplier"."s_name") AS "s_name", "part"."p_name", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("part"."p_name") AS "PART_NAME", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", AVG(CASE WHEN "lineitem"."l_returnflag" = 'R' THEN CAST("lineitem"."l_quantity" AS DECIMAL(15, 2)) ELSE NULL END) AS "AVG_RETURN_AMT", LISTAGG(DISTINCT CONCAT("nation"."n_name", ': ', "supplier"."s_comment"), '; ') AS "SUPPLIER_COMMENTS", ANY_VALUE(REGEXP_REPLACE(UPPER("part"."p_name"), '[^A-Z0-9 ]', '')) AS "SANITIZED_PART_NAME", AVG("lineitem"."l_quantity") AS "FD_COL_9"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t" ON "customer"."c_custkey" = "t"."o_custkey") ON "lineitem"."l_orderkey" = "t"."o_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "supplier"."s_name", "part"."p_name"