SELECT COALESCE(ANY_VALUE("Users"."Id"), ANY_VALUE("Users"."Id")) AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", COUNT("Posts"."Id") AS "POSTSCREATED", SUM(CASE WHEN "Posts"."Score" IS NOT NULL THEN CAST("Posts"."Score" AS INTEGER) ELSE 0 END) AS "TOTALSCORE", AVG(CASE WHEN "Posts"."ViewCount" IS NOT NULL THEN CAST("Posts"."ViewCount" AS INTEGER) ELSE 0 END) AS "AVGVIEWCOUNT"
FROM "STACK"."Posts"
RIGHT JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName"
HAVING SUM(CASE WHEN "Posts"."Score" IS NOT NULL THEN CAST("Posts"."Score" AS INTEGER) ELSE 0 END) > 0