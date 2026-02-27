SELECT COALESCE(ANY_VALUE("Users"."Id"), ANY_VALUE("Users"."Id")) AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", COUNT(DISTINCT "Posts"."Id") AS "TOTALPOSTS", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONS", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERS", AVG(CASE WHEN "Posts"."Score" IS NOT NULL THEN CAST("Posts"."Score" AS INTEGER) ELSE 0 END) AS "AVGPOSTSCORE", AVG(CASE WHEN "Posts"."ViewCount" IS NOT NULL THEN CAST("Posts"."ViewCount" AS INTEGER) ELSE 0 END) AS "AVGVIEWCOUNT", COUNT(DISTINCT "Comments"."Id") AS "TOTALCOMMENTS", COUNT(DISTINCT "Badges"."Id") AS "TOTALBADGES"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Badges" ON "Users"."Id" = "Badges"."UserId"
WHERE "Users"."Reputation" > 1000
GROUP BY "Users"."Id", "Users"."DisplayName"