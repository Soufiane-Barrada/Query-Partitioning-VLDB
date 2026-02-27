SELECT COALESCE("t5"."PART_NAME", "t5"."PART_NAME") AS "PART_NAME", "t5"."SUPPLIER_COUNT", "t5"."AVERAGE_SUPPLY_COST", "t5"."SHORT_COMMENT", "t5"."NATION_REGION"
FROM (SELECT "t1"."p_name", "s1"."n_name", "s1"."r_name", "t1"."p_comment", ANY_VALUE("t1"."p_name") AS "PART_NAME", COUNT(DISTINCT "s1"."s_suppkey") AS "SUPPLIER_COUNT", AVG("partsupp"."ps_supplycost") AS "AVERAGE_SUPPLY_COST", ANY_VALUE(SUBSTRING("t1"."p_comment", 1, 15)) AS "SHORT_COMMENT", ANY_VALUE(CONCAT("s1"."n_name", ' (', "s1"."r_name", ')')) AS "NATION_REGION"
FROM (SELECT *
FROM "TPCH"."part"
WHERE "p_type" LIKE '%brass%' AND ("p_retailprice" >= 10.00 AND "p_retailprice" <= 100.00)) AS "t1"
INNER JOIN ("s1" INNER JOIN "TPCH"."partsupp" ON "s1"."s_suppkey" = "partsupp"."ps_suppkey") ON "t1"."p_partkey" = "partsupp"."ps_partkey"
GROUP BY "t1"."p_name", "s1"."n_name", "s1"."r_name", "t1"."p_comment"
HAVING COUNT(DISTINCT "s1"."s_suppkey") > 5
ORDER BY 7 DESC NULLS FIRST, 5) AS "t5"