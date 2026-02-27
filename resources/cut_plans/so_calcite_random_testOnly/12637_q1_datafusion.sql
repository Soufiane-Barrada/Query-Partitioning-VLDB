SELECT COALESCE(ANY_VALUE("PostTypes"."Id"), ANY_VALUE("PostTypes"."Id")) AS "POSTTYPEID", ANY_VALUE("PostTypes"."Name") AS "POSTTYPENAME", COUNT("Posts"."Id") AS "POSTCOUNT", SUM(CASE WHEN "Posts"."ViewCount" IS NOT NULL THEN CAST("Posts"."ViewCount" AS INTEGER) ELSE 0 END) AS "TOTALVIEWS", SUM(CASE WHEN "Posts"."Score" IS NOT NULL THEN CAST("Posts"."Score" AS INTEGER) ELSE 0 END) AS "TOTALSCORE"
FROM "STACK"."Posts"
RIGHT JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id"
GROUP BY "PostTypes"."Id", "PostTypes"."Name"