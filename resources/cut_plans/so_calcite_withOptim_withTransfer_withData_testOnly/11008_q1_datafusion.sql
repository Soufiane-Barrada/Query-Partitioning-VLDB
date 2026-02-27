SELECT COALESCE(ANY_VALUE("PostTypes"."Name"), ANY_VALUE("PostTypes"."Name")) AS "POSTTYPE", COUNT(*) AS "POSTCOUNT", AVG("Posts"."Score") AS "AVERAGESCORE"
FROM "STACK"."PostTypes"
INNER JOIN "STACK"."Posts" ON "PostTypes"."Id" = "Posts"."PostTypeId"
GROUP BY "PostTypes"."Name"