SELECT COALESCE("t"."Id", "t"."Id") AS "Id", "t"."OwnerUserId" AS "OWNERUSERID", "Votes"."Id" AS "Id0", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END AS "FD_COL_3", CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END AS "FD_COL_4"
FROM "STACK"."Votes"
RIGHT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= TIMESTAMP '2021-01-01 00:00:00') AS "t" ON "Votes"."PostId" = "t"."Id"