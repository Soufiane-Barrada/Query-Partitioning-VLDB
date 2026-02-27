SELECT COALESCE("Posts"."OwnerUserId", "Posts"."OwnerUserId") AS "OwnerUserId", COUNT(*) AS "UPVOTES"
FROM (SELECT *
FROM "STACK"."Votes"
WHERE CAST("VoteTypeId" AS INTEGER) = 2) AS "t"
INNER JOIN "STACK"."Posts" ON "t"."PostId" = "Posts"."Id"
GROUP BY "Posts"."OwnerUserId"