SELECT COALESCE("S_SUPPKEY", "S_SUPPKEY") AS "S_SUPPKEY", "S_NAME", "TOTALSUPPLYCOST"
FROM (SELECT "t2"."s_suppkey" AS "S_SUPPKEY", "t2"."s_name" AS "S_NAME", "t2"."TOTALSUPPLYCOST"
FROM (SELECT "supplier0"."s_suppkey", "supplier0"."s_name", "supplier0"."s_address", "supplier0"."s_nationkey", "supplier0"."s_phone", "supplier0"."s_acctbal", "supplier0"."s_comment", "s1"."S_SUPPKEY", "s1"."TOTALSUPPLYCOST"
FROM "TPCH"."supplier" AS "supplier0"
INNER JOIN "s1" ON "supplier0"."s_suppkey" = "s1"."S_SUPPKEY"
ORDER BY "s1"."TOTALSUPPLYCOST" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"
ORDER BY "t2"."TOTALSUPPLYCOST") AS "t4"