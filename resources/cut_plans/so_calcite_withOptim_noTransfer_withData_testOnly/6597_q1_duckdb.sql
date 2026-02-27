SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."DisplayName" AS "DISPLAYNAME", "t"."Reputation" AS "REPUTATION", "Posts"."Id" AS "Id0", CASE WHEN "Posts"."ViewCount" > 1000 THEN 1 ELSE 0 END AS "FD_COL_4", CASE WHEN "Posts"."Score" > 50 THEN 1 ELSE 0 END AS "FD_COL_5"
FROM "STACK"."Posts"
RIGHT JOIN (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 100) AS "t" ON "Posts"."OwnerUserId" = "t"."Id"