SELECT COALESCE(ANY_VALUE("Users"."Id"), ANY_VALUE("Users"."Id")) AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT"
FROM "STACK"."Votes"
RIGHT JOIN "STACK"."Users" ON "Votes"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName"