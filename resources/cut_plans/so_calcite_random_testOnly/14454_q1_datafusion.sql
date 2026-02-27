SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", ANY_VALUE("PostTypes"."Name") AS "POSTTYPE", COUNT(*) AS "TOTALPOSTS", AVG("Posts"."Score") AS "AVERAGESCORE", MAX("Posts"."ViewCount") AS "MAXVIEWS", COUNT(DISTINCT "Comments"."Id") AS "TOTALCOMMENTS"
FROM "STACK"."Comments"
RIGHT JOIN ("STACK"."Posts" INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id") ON "Comments"."PostId" = "Posts"."Id"
GROUP BY "PostTypes"."Name"