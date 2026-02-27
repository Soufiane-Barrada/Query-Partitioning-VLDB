SELECT COALESCE("Users"."Id", "Users"."Id") AS "Id", "Users"."Reputation", "Posts"."Id" AS "Id0", CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END AS "FD_COL_3", CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_4", CASE WHEN "Votes"."CreationDate" IS NOT NULL THEN 1 ELSE 0 END AS "FD_COL_5", "Posts"."Score", "Posts"."CreationDate" AS "CreationDate0", "Votes"."UserId"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"