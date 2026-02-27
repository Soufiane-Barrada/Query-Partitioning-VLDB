SELECT COALESCE("t2"."C_CUSTKEY", "t2"."C_CUSTKEY") AS "C_CUSTKEY", "t2"."CUSTOMER_EXPENDITURE", "t2"."$f2" AS "FD_COL_2", "t7"."P_PARTKEY", "t7"."P_NAME", "t7"."TOTAL_REVENUE", "t10"."S_SUPPKEY", "t10"."SUPPLIER_REVENUE"
FROM (SELECT "customer"."c_custkey" AS "C_CUSTKEY", SUM("orders"."o_totalprice") AS "CUSTOMER_EXPENDITURE", SUM("orders"."o_totalprice") > 1000.00 AS "$f2"
FROM "TPCH"."customer"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1998-01-01') AS "t" INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey"
HAVING SUM("orders"."o_totalprice") > 1000.00) AS "t2"
CROSS JOIN ((SELECT "part0"."p_partkey" AS "P_PARTKEY", "part0"."p_name" AS "P_NAME", "t5"."TOTAL_REVENUE"
FROM (SELECT "part"."p_partkey" AS "P_PARTKEY", SUM("t3"."l_extendedprice" * (1 - "t3"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."part"
INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1998-01-01') AS "t3" ON "part"."p_partkey" = "t3"."l_partkey"
GROUP BY "part"."p_partkey") AS "t5"
INNER JOIN "TPCH"."part" AS "part0" ON "t5"."P_PARTKEY" = "part0"."p_partkey"
ORDER BY "t5"."TOTAL_REVENUE" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t7" INNER JOIN (SELECT "supplier"."s_suppkey" AS "S_SUPPKEY", SUM("t8"."l_extendedprice" * (1 - "t8"."l_discount")) AS "SUPPLIER_REVENUE"
FROM "TPCH"."supplier"
INNER JOIN ("TPCH"."partsupp" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1998-01-01') AS "t8" ON "partsupp"."ps_partkey" = "t8"."l_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "supplier"."s_suppkey") AS "t10" ON "t7"."P_PARTKEY" = "t10"."S_SUPPKEY")