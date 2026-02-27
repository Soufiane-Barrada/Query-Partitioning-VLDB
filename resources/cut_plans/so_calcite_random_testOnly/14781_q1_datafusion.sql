SELECT COALESCE("Posts"."Id", "Posts"."Id") AS "Id", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_1", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END AS "FD_COL_2"
FROM "STACK"."Votes"
RIGHT JOIN "STACK"."Posts" ON "Votes"."PostId" = "Posts"."Id"