SELECT COALESCE("Tags"."TagName", "Tags"."TagName") AS "TAGNAME", "Posts"."Id" AS "Id0", CASE WHEN "Posts"."ViewCount" IS NOT NULL THEN CAST("Posts"."ViewCount" AS INTEGER) ELSE 0 END AS "FD_COL_2"
FROM "STACK"."Posts"
RIGHT JOIN "STACK"."Tags" ON "Posts"."Tags" LIKE '%' || "Tags"."TagName" || '%'