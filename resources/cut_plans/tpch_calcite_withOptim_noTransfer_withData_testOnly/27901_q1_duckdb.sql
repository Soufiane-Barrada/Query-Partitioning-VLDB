SELECT COALESCE("t0"."p_brand", "t0"."p_brand") AS "P_BRAND", COUNT(DISTINCT "t"."s_name") AS "SUPPLIER_COUNT", AVG("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "AVG_PRICE", MAX("t1"."l_shipdate") AS "LAST_SHIP_DATE", MIN("t1"."l_shipdate") AS "FIRST_SHIP_DATE", LISTAGG(DISTINCT CONCAT("t"."s_name", ' (', "t"."s_phone", ')'), ', ') AS "SUPPLIER_INFO", COUNT(DISTINCT "t1"."l_orderkey") AS "FD_COL_6"
FROM (SELECT *
FROM "TPCH"."supplier"
WHERE "s_acctbal" > 10000.00) AS "t"
INNER JOIN ((SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%metal%') AS "t0" INNER JOIN ("TPCH"."partsupp" INNER JOIN (SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" <= DATE '1997-12-31') AS "t1" ON "partsupp"."ps_partkey" = "t1"."l_partkey") ON "t0"."p_partkey" = "partsupp"."ps_partkey") ON "t"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "t0"."p_brand"
HAVING COUNT(DISTINCT "t1"."l_orderkey") > 5