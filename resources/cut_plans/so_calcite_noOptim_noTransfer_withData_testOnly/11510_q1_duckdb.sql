SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", "Posts"."Score", "Votes"."UserId"
FROM "STACK"."Posts"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"