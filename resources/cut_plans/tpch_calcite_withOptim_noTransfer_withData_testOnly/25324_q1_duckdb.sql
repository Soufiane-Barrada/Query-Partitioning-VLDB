SELECT COALESCE("t0"."p_name", "t0"."p_name") AS "p_name", "t"."s_name", "customer"."c_name", "t1"."o_orderkey", ANY_VALUE("t0"."p_name") AS "PART_NAME", ANY_VALUE("t"."s_name") AS "SUPPLIER_NAME", ANY_VALUE("customer"."c_name") AS "CUSTOMER_NAME", ANY_VALUE("t1"."o_orderkey") AS "ORDER_ID", COUNT(*) AS "LINE_ITEM_COUNT", SUM("lineitem"."l_extendedprice") AS "TOTAL_EXTENDED_PRICE", AVG("lineitem"."l_discount") AS "AVERAGE_DISCOUNT", MAX("lineitem"."l_shipdate") AS "LATEST_SHIP_DATE", LISTAGG(DISTINCT "region"."r_name", ', ') AS "REGIONS_SUPPLIED", LISTAGG(DISTINCT "t0"."p_comment", '|') AS "PART_COMMENTS"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 500.00) AS "t" ON "nation"."n_nationkey" = "t"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_size" > 10) AS "t0" INNER JOIN "TPCH"."partsupp" ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ((SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderstatus" = 'O') AS "t1" INNER JOIN "TPCH"."customer" ON "t1"."o_custkey" = "customer"."c_custkey") ON "lineitem"."l_orderkey" = "t1"."o_orderkey") ON "t0"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "t0"."p_name", "t"."s_name", "customer"."c_name", "t1"."o_orderkey"