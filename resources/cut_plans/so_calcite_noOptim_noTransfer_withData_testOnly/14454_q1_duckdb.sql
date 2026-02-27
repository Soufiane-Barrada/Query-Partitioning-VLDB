SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", "Posts"."Score", "Posts"."ViewCount", "Comments"."Id" AS "Id1"
FROM "STACK"."Posts"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"