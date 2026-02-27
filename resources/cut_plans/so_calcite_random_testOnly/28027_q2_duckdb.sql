SELECT COALESCE("t9"."TITLE", "t9"."TITLE") AS "TITLE", "t9"."TOTALPOSTS", "t9"."TOTALUPVOTES", "t9"."TOTALDOWNVOTES", "t9"."TOTALCLOSEDPOSTS"
FROM (SELECT "t5"."Title" AS "TITLE", CASE WHEN "t5"."POSTCOUNT" IS NOT NULL THEN CAST("t5"."POSTCOUNT" AS BIGINT) ELSE 0 END AS "TOTALPOSTS", CASE WHEN "s1"."UPVOTECOUNT" IS NOT NULL THEN CAST("s1"."UPVOTECOUNT" AS INTEGER) ELSE 0 END AS "TOTALUPVOTES", CASE WHEN "s1"."DOWNVOTECOUNT" IS NOT NULL THEN CAST("s1"."DOWNVOTECOUNT" AS INTEGER) ELSE 0 END AS "TOTALDOWNVOTES", CASE WHEN "t7"."CLOSEDPOSTCOUNT" IS NOT NULL THEN CAST("t7"."CLOSEDPOSTCOUNT" AS INTEGER) ELSE 0 END AS "TOTALCLOSEDPOSTS"
FROM (SELECT "Posts"."Id" AS "Id", "Posts"."PostTypeId" AS "PostTypeId", "Posts"."AcceptedAnswerId" AS "AcceptedAnswerId", "Posts"."ParentId" AS "ParentId", "Posts"."CreationDate" AS "CreationDate", "Posts"."Score" AS "Score", "Posts"."ViewCount" AS "ViewCount", "Posts"."Body" AS "Body", "Posts"."OwnerUserId" AS "OwnerUserId", "Posts"."OwnerDisplayName" AS "OwnerDisplayName", "Posts"."LastEditorUserId" AS "LastEditorUserId", "Posts"."LastEditorDisplayName" AS "LastEditorDisplayName", "Posts"."LastEditDate" AS "LastEditDate", "Posts"."LastActivityDate" AS "LastActivityDate", "Posts"."Title" AS "Title", "Posts"."Tags" AS "Tags", "Posts"."AnswerCount" AS "AnswerCount", "Posts"."CommentCount" AS "CommentCount", "Posts"."FavoriteCount" AS "FavoriteCount", "Posts"."ClosedDate" AS "ClosedDate", "Posts"."CommunityOwnedDate" AS "CommunityOwnedDate", "Posts"."ContentLicense" AS "ContentLicense", "t4"."Id" AS "ID", "t4"."POSTCOUNT" AS "POSTCOUNT"
FROM "STACK"."Posts"
LEFT JOIN (SELECT "Id", COUNT(*) AS "POSTCOUNT"
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) IN (1, 2)
GROUP BY "Id") AS "t4" ON "Posts"."Id" = "t4"."Id"
WHERE CASE WHEN "t4"."POSTCOUNT" IS NOT NULL THEN CAST("t4"."POSTCOUNT" AS BIGINT) ELSE 0 END > 1) AS "t5"
LEFT JOIN "s1" ON "t5"."OwnerUserId" = "s1"."USERID"
LEFT JOIN (SELECT "Tags"."TagName" AS "TAGNAME", COUNT(*) AS "POSTCOUNT", SUM(CASE WHEN CAST("Posts1"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONCOUNT", SUM(CASE WHEN CAST("Posts1"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERCOUNT", SUM(CASE WHEN CAST("PostHistory"."PostHistoryTypeId" AS INTEGER) = 10 THEN 1 ELSE 0 END) AS "CLOSEDPOSTCOUNT"
FROM "STACK"."PostHistory"
RIGHT JOIN "STACK"."Posts" AS "Posts1" ON "PostHistory"."PostId" = "Posts1"."Id"
INNER JOIN "STACK"."Tags" ON "Posts1"."Tags" LIKE '%' || "Tags"."TagName" || '%'
GROUP BY "Tags"."TagName") AS "t7" ON "t5"."Tags" LIKE '%' || "t7"."TAGNAME" || '%'
ORDER BY 2 DESC NULLS FIRST, 3 DESC NULLS FIRST, 4
FETCH NEXT 10 ROWS ONLY) AS "t9"