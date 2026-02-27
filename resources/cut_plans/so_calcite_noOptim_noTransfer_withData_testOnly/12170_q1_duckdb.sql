SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", "Posts"."Score", "Posts"."ViewCount", "Posts"."AnswerCount", "Posts"."CommentCount", "Posts"."FavoriteCount", "Posts"."OwnerUserId"
FROM "STACK"."Posts"
INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"