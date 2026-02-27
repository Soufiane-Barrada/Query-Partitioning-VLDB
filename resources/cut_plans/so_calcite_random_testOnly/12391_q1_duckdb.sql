SELECT COALESCE(ANY_VALUE("Users"."Id"), ANY_VALUE("Users"."Id")) AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", COUNT("Posts"."Id") AS "POSTCOUNT", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONCOUNT", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERCOUNT", SUM("Posts"."ViewCount") AS "TOTALVIEWS", AVG("Posts"."Score") AS "AVERAGESCORE", COUNT("Posts"."Id") > 0 AS "FD_COL_7"
FROM "STACK"."Posts"
RIGHT JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName"
HAVING COUNT("Posts"."Id") > 0