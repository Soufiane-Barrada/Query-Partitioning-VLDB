SELECT COALESCE("t"."DisplayName", "t"."DisplayName") AS "DisplayName", COUNT(DISTINCT "Posts"."Id") AS "POSTCOUNT"
FROM (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t"
INNER JOIN "STACK"."Posts" ON "t"."Id" = "Posts"."OwnerUserId"
GROUP BY "t"."DisplayName"