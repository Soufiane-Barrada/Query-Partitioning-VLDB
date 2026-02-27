SELECT COALESCE("Users"."Id", "Users"."Id") AS "Id", "Users"."Reputation" AS "REPUTATION", "Posts"."Id" AS "Id0", "Votes"."BountyAmount"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId" AND CAST("Votes"."VoteTypeId" AS INTEGER) = 8