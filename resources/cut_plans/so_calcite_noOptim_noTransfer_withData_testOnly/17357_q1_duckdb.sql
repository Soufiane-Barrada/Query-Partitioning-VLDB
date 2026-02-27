SELECT COALESCE("Users"."DisplayName", "Users"."DisplayName") AS "DISPLAYNAME", COUNT(DISTINCT "Posts"."Id") AS "POSTCOUNT"
FROM "STACK"."Users"
INNER JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
WHERE "Users"."Reputation" > 1000
GROUP BY "Users"."DisplayName"