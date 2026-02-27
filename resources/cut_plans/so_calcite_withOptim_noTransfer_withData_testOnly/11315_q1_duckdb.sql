SELECT COALESCE("PostTypes"."Name", "PostTypes"."Name") AS "Name", ANY_VALUE("PostTypes"."Name") AS "POSTTYPE", COUNT(*) AS "TOTALPOSTS", SUM("Posts"."Score") AS "FD_COL_3", SUM("Posts"."ViewCount") AS "FD_COL_4", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "FD_COL_5", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "FD_COL_6"
FROM "STACK"."Votes"
RIGHT JOIN ("STACK"."Posts" INNER JOIN "STACK"."PostTypes" ON "Posts"."PostTypeId" = "PostTypes"."Id") ON "Votes"."PostId" = "Posts"."Id"
GROUP BY "PostTypes"."Name"