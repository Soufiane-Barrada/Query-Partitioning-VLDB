SELECT COALESCE("Tags"."TagName", "Tags"."TagName") AS "TAGNAME", COUNT(*) AS "POSTCOUNT", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONCOUNT", SUM(CASE WHEN CAST("Posts"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERCOUNT", SUM(CASE WHEN CAST("PostHistory"."PostHistoryTypeId" AS INTEGER) = 10 THEN 1 ELSE 0 END) AS "CLOSEDPOSTCOUNT"
FROM "STACK"."Posts"
INNER JOIN "STACK"."Tags" ON "Posts"."Tags" LIKE '%' || "Tags"."TagName" || '%'
LEFT JOIN "STACK"."PostHistory" ON "Posts"."Id" = "PostHistory"."PostId"
GROUP BY "Tags"."TagName"