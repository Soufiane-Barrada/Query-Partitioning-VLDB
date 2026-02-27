SELECT COALESCE("Users"."Id", "Users"."Id") AS "Id", "Users"."DisplayName" AS "DISPLAYNAME", "Votes"."Id" AS "Id0", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_3", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END AS "FD_COL_4"
FROM "STACK"."Votes"
RIGHT JOIN "STACK"."Users" ON "Votes"."UserId" = "Users"."Id"