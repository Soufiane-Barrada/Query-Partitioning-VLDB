SELECT COALESCE("Tags"."TagName", "Tags"."TagName") AS "TagName", COUNT(*) AS "POSTCOUNT", SUM("Posts"."ViewCount") AS "TOTALVIEWS"
FROM "STACK"."Tags"
INNER JOIN "STACK"."Posts" ON "Posts"."Tags" LIKE '%' || "Tags"."TagName" || '%'
GROUP BY "Tags"."TagName"