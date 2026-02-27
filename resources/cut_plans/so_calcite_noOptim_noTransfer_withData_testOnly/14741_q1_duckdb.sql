SELECT COALESCE("t1"."USERID", "t1"."USERID") AS "USERID", "t1"."DISPLAYNAME", "t1"."REPUTATION", "t1"."POSTCOUNT", "t1"."UPVOTES", "t1"."DOWNVOTES", "t5"."AVGREPUTATION", "t5"."AVGPOSTCOUNT", "t5"."AVGUPVOTES", "t5"."AVGDOWNVOTES"
FROM (SELECT ANY_VALUE("Users"."Id") AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", "Users"."Reputation" AS "REPUTATION", COUNT(DISTINCT "Posts"."Id") AS "POSTCOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES"
FROM "STACK"."Users"
LEFT JOIN "STACK"."Posts" ON "Users"."Id" = "Posts"."OwnerUserId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
GROUP BY "Users"."Id", "Users"."DisplayName", "Users"."Reputation") AS "t1",
(SELECT AVG("Users0"."Reputation") AS "AVGREPUTATION", AVG(COUNT(DISTINCT "Posts0"."Id")) AS "AVGPOSTCOUNT", AVG(SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END)) AS "AVGUPVOTES", AVG(SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END)) AS "AVGDOWNVOTES"
FROM "STACK"."Users" AS "Users0"
LEFT JOIN "STACK"."Posts" AS "Posts0" ON "Users0"."Id" = "Posts0"."OwnerUserId"
LEFT JOIN "STACK"."Votes" AS "Votes0" ON "Posts0"."Id" = "Votes0"."PostId"
GROUP BY "Users0"."Id", "Users0"."DisplayName", "Users0"."Reputation") AS "t5"
WHERE "t1"."REPUTATION" > 1000