SELECT COALESCE("Users"."DisplayName", "Users"."DisplayName") AS "DISPLAYNAME", "Posts"."ViewCount" AS "FD_COL_1"
FROM "STACK"."Users"
INNER JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"