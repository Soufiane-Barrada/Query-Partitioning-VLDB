SELECT COALESCE(ANY_VALUE("t"."p_name"), ANY_VALUE("t"."p_name")) AS "PART_NAME", ANY_VALUE("supplier"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", ANY_VALUE("t0"."o_orderdate") AS "ORDER_DATE", COUNT(DISTINCT "lineitem"."l_orderkey") AS "ORDER_COUNT", ROUND(SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")), 2) AS "TOTAL_REVENUE", LISTAGG(DISTINCT "region"."r_name", ', ') AS "REGIONS_INVOLVED", MAX(CASE WHEN "lineitem"."l_returnflag" = 'R' THEN 'Returned    ' ELSE 'Not Returned' END) AS "RETURN_STATUS"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_name" LIKE '%steel%') AS "t"
INNER JOIN "TPCH"."partsupp" ON "t"."p_partkey" = "partsupp"."ps_partkey"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t0" ON "customer"."c_custkey" = "t0"."o_custkey" INNER JOIN ("TPCH"."region" INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey") ON "t0"."o_orderkey" = "lineitem"."l_orderkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey" AND "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
GROUP BY "t"."p_name", "supplier"."s_name", "customer"."c_name", "t0"."o_orderdate"