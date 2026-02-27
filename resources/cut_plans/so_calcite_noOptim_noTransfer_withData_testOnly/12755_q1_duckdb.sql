SELECT COALESCE("Posts"."Id", "Posts"."Id") AS "Id", "Posts"."PostTypeId" AS "POSTTYPEID", "Comments"."Id" AS "Id0", "Votes"."Id" AS "Id1", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_4", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END AS "FD_COL_5", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 4 THEN 1 ELSE 0 END AS "FD_COL_6", "Posts"."CreationDate"
FROM "STACK"."Posts"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"