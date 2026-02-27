SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", ANY_VALUE("PostTypes"."Name") AS "POSTTYPE", COUNT(*) AS "TOTALPOSTS", AVG("Posts"."Score") AS "AVERAGESCORE", SUM("Posts"."ViewCount") AS "TOTALVIEWS", AVG("Posts"."AnswerCount") AS "AVERAGEANSWERCOUNT", AVG("Posts"."CommentCount") AS "AVERAGECOMMENTCOUNT", AVG("Posts"."FavoriteCount") AS "AVERAGEFAVORITECOUNT", COUNT(DISTINCT "Posts"."OwnerUserId") AS "UNIQUEUSERS"
FROM "STACK"."PostTypes"
INNER JOIN "STACK"."Posts" ON "PostTypes"."Id" = "Posts"."PostTypeId"
GROUP BY "PostTypes"."Name"