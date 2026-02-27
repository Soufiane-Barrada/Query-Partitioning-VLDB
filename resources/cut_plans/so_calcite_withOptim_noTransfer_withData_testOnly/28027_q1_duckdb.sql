SELECT COALESCE("t1"."Title", "t1"."Title") AS "TITLE", CASE WHEN "t1"."POSTCOUNT" IS NOT NULL THEN CAST("t1"."POSTCOUNT" AS BIGINT) ELSE 0 END AS "TOTALPOSTS", CASE WHEN "t4"."UPVOTECOUNT" IS NOT NULL THEN CAST("t4"."UPVOTECOUNT" AS INTEGER) ELSE 0 END AS "TOTALUPVOTES", CASE WHEN "t4"."DOWNVOTECOUNT" IS NOT NULL THEN CAST("t4"."DOWNVOTECOUNT" AS INTEGER) ELSE 0 END AS "TOTALDOWNVOTES", CASE WHEN "t6"."CLOSEDPOSTCOUNT" IS NOT NULL THEN CAST("t6"."CLOSEDPOSTCOUNT" AS INTEGER) ELSE 0 END AS "TOTALCLOSEDPOSTS"
FROM (SELECT "Posts"."Id" AS "Id", "Posts"."PostTypeId" AS "PostTypeId", "Posts"."AcceptedAnswerId" AS "AcceptedAnswerId", "Posts"."ParentId" AS "ParentId", "Posts"."CreationDate" AS "CreationDate", "Posts"."Score" AS "Score", "Posts"."ViewCount" AS "ViewCount", "Posts"."Body" AS "Body", "Posts"."OwnerUserId" AS "OwnerUserId", "Posts"."OwnerDisplayName" AS "OwnerDisplayName", "Posts"."LastEditorUserId" AS "LastEditorUserId", "Posts"."LastEditorDisplayName" AS "LastEditorDisplayName", "Posts"."LastEditDate" AS "LastEditDate", "Posts"."LastActivityDate" AS "LastActivityDate", "Posts"."Title" AS "Title", "Posts"."Tags" AS "Tags", "Posts"."AnswerCount" AS "AnswerCount", "Posts"."CommentCount" AS "CommentCount", "Posts"."FavoriteCount" AS "FavoriteCount", "Posts"."ClosedDate" AS "ClosedDate", "Posts"."CommunityOwnedDate" AS "CommunityOwnedDate", "Posts"."ContentLicense" AS "ContentLicense", "t0"."Id" AS "ID", "t0"."POSTCOUNT" AS "POSTCOUNT"
FROM "STACK"."Posts"
LEFT JOIN (SELECT "Id", COUNT(*) AS "POSTCOUNT"
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) IN (1, 2)
GROUP BY "Id") AS "t0" ON "Posts"."Id" = "t0"."Id"
WHERE CASE WHEN "t0"."POSTCOUNT" IS NOT NULL THEN CAST("t0"."POSTCOUNT" AS BIGINT) ELSE 0 END > 1) AS "t1"
LEFT JOIN (SELECT ANY_VALUE("Users"."Id") AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT"
FROM "STACK"."Votes"
RIGHT JOIN "STACK"."Users" ON "Votes"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName") AS "t4" ON "t1"."OwnerUserId" = "t4"."USERID"
LEFT JOIN (SELECT "Tags"."TagName" AS "TAGNAME", COUNT(*) AS "POSTCOUNT", SUM(CASE WHEN CAST("Posts1"."PostTypeId" AS INTEGER) = 1 THEN 1 ELSE 0 END) AS "QUESTIONCOUNT", SUM(CASE WHEN CAST("Posts1"."PostTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "ANSWERCOUNT", SUM(CASE WHEN CAST("PostHistory"."PostHistoryTypeId" AS INTEGER) = 10 THEN 1 ELSE 0 END) AS "CLOSEDPOSTCOUNT"
FROM "STACK"."PostHistory"
RIGHT JOIN "STACK"."Posts" AS "Posts1" ON "PostHistory"."PostId" = "Posts1"."Id"
INNER JOIN "STACK"."Tags" ON "Posts1"."Tags" LIKE '%' || "Tags"."TagName" || '%'
GROUP BY "Tags"."TagName") AS "t6" ON "t1"."Tags" LIKE '%' || "t6"."TAGNAME" || '%'