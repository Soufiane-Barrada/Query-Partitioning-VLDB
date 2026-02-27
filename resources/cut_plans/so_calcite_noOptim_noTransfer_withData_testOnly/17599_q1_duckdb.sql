SELECT COALESCE("Users"."DisplayName", "Users"."DisplayName") AS "DISPLAYNAME", "Posts"."Id" AS "Id0", CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END AS "FD_COL_2", CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_3", "Users"."Reputation"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"