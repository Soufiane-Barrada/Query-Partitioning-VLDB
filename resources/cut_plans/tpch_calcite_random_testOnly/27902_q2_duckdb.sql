SELECT COALESCE("t5"."P_NAME", "t5"."P_NAME") AS "P_NAME", "t5"."S_NAME", "t5"."CUSTOMER_COUNT", "t5"."ORDER_COUNT", "t5"."TOTAL_QUANTITY", "t5"."AVG_PRICE", "t5"."REGIONS_SERVED"
FROM (SELECT "s1"."p_name" AS "P_NAME", "s1"."s_name" AS "S_NAME", "t2"."CUSTOMER_COUNT", COUNT(DISTINCT "s1"."o_orderkey") AS "ORDER_COUNT", SUM("s1"."l_quantity") AS "TOTAL_QUANTITY", AVG("s1"."l_extendedprice") AS "AVG_PRICE", LISTAGG(DISTINCT "s1"."r_name", ', ') AS "REGIONS_SERVED"
FROM (SELECT COUNT(DISTINCT "c_custkey") AS "CUSTOMER_COUNT"
FROM "TPCH"."customer"
WHERE "c_acctbal" > 1000.00) AS "t2",
"s1"
GROUP BY "s1"."p_name", "s1"."s_name", "t2"."CUSTOMER_COUNT"
ORDER BY 5 DESC NULLS FIRST, 6 DESC NULLS FIRST) AS "t5"