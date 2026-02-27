SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", ANY_VALUE("PostTypes"."Name") AS "POSTTYPE", COUNT(*) AS "TOTALPOSTS", AVG("t"."Score") AS "AVERAGESCORE", SUM("t"."ViewCount") AS "TOTALVIEWS", COUNT(DISTINCT "t"."OwnerUserId") AS "UNIQUEUSERS"
FROM "STACK"."Users"
RIGHT JOIN ((SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= TIMESTAMP '2023-01-01 00:00:00') AS "t" INNER JOIN "STACK"."PostTypes" ON "t"."PostTypeId" = "PostTypes"."Id") ON "Users"."Id" = "t"."OwnerUserId"
GROUP BY "PostTypes"."Name"