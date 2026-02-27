SELECT COALESCE("t9"."TITLE", "t9"."TITLE") AS "TITLE", "t9"."TOTALPOSTS", "t9"."TOTALUPVOTES", "t9"."TOTALDOWNVOTES", "t9"."TOTALCLOSEDPOSTS"
FROM (SELECT "t4"."Title" AS "TITLE", CASE WHEN "t4"."POSTCOUNT" IS NOT NULL THEN CAST("t4"."POSTCOUNT" AS BIGINT) ELSE 0 END AS "TOTALPOSTS", CASE WHEN "t7"."UPVOTECOUNT" IS NOT NULL THEN CAST("t7"."UPVOTECOUNT" AS INTEGER) ELSE 0 END AS "TOTALUPVOTES", CASE WHEN "t7"."DOWNVOTECOUNT" IS NOT NULL THEN CAST("t7"."DOWNVOTECOUNT" AS INTEGER) ELSE 0 END AS "TOTALDOWNVOTES", CASE WHEN "s1"."CLOSEDPOSTCOUNT" IS NOT NULL THEN CAST("s1"."CLOSEDPOSTCOUNT" AS INTEGER) ELSE 0 END AS "TOTALCLOSEDPOSTS"
FROM (SELECT "Posts0"."Id" AS "Id", "Posts0"."PostTypeId" AS "PostTypeId", "Posts0"."AcceptedAnswerId" AS "AcceptedAnswerId", "Posts0"."ParentId" AS "ParentId", "Posts0"."CreationDate" AS "CreationDate", "Posts0"."Score" AS "Score", "Posts0"."ViewCount" AS "ViewCount", "Posts0"."Body" AS "Body", "Posts0"."OwnerUserId" AS "OwnerUserId", "Posts0"."OwnerDisplayName" AS "OwnerDisplayName", "Posts0"."LastEditorUserId" AS "LastEditorUserId", "Posts0"."LastEditorDisplayName" AS "LastEditorDisplayName", "Posts0"."LastEditDate" AS "LastEditDate", "Posts0"."LastActivityDate" AS "LastActivityDate", "Posts0"."Title" AS "Title", "Posts0"."Tags" AS "Tags", "Posts0"."AnswerCount" AS "AnswerCount", "Posts0"."CommentCount" AS "CommentCount", "Posts0"."FavoriteCount" AS "FavoriteCount", "Posts0"."ClosedDate" AS "ClosedDate", "Posts0"."CommunityOwnedDate" AS "CommunityOwnedDate", "Posts0"."ContentLicense" AS "ContentLicense", "t3"."Id" AS "ID", "t3"."POSTCOUNT" AS "POSTCOUNT"
FROM "STACK"."Posts" AS "Posts0"
LEFT JOIN (SELECT "Id", COUNT(*) AS "POSTCOUNT"
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) IN (1, 2)
GROUP BY "Id") AS "t3" ON "Posts0"."Id" = "t3"."Id"
WHERE CASE WHEN "t3"."POSTCOUNT" IS NOT NULL THEN CAST("t3"."POSTCOUNT" AS BIGINT) ELSE 0 END > 1) AS "t4"
LEFT JOIN (SELECT ANY_VALUE("Users"."Id") AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT"
FROM "STACK"."Votes"
RIGHT JOIN "STACK"."Users" ON "Votes"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName") AS "t7" ON "t4"."OwnerUserId" = "t7"."USERID"
LEFT JOIN "s1" ON "t4"."Tags" LIKE '%' || "s1"."TAGNAME" || '%'
ORDER BY 2 DESC NULLS FIRST, 3 DESC NULLS FIRST, 4
FETCH NEXT 10 ROWS ONLY) AS "t9"