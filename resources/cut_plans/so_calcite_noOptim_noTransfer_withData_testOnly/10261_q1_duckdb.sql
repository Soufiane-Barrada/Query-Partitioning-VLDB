SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", CASE WHEN "Posts"."Score" > 0 THEN 1 ELSE 0 END AS "FD_COL_1", "Posts"."ViewCount", "Posts"."Score", "Posts"."AnswerCount" AS "FD_COL_4", "Posts"."CommentCount" AS "FD_COL_5", "Posts"."OwnerUserId", "Users"."Reputation"
FROM "STACK"."Posts"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"
INNER JOIN "STACK"."Users" ON "Posts"."OwnerUserId" = "Users"."Id"