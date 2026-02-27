SELECT COALESCE("t10"."USERID", "t10"."USERID") AS "USERID", "t10"."DISPLAYNAME", "t10"."REPUTATION", "t10"."QUESTIONCOUNT", "t10"."ANSWERCOUNT", "t10"."ACCEPTEDANSWERCOUNT", "t10"."TOTALCOMMENTCOUNT", "t10"."TOTALUPVOTES", "t10"."TOTALDOWNVOTES"
FROM (SELECT ANY_VALUE("t3"."Id") AS "USERID", "t3"."DisplayName" AS "DISPLAYNAME", "t3"."Reputation" AS "REPUTATION", COUNT(DISTINCT "t3"."Id0") AS "QUESTIONCOUNT", COUNT(DISTINCT "t3"."Id1") AS "ANSWERCOUNT", SUM(CASE WHEN "t3"."AcceptedAnswerId0" IS NOT NULL THEN 1 ELSE 0 END) AS "ACCEPTEDANSWERCOUNT", SUM(CASE WHEN "t4"."COMMENT_COUNT" IS NOT NULL THEN CAST("t4"."COMMENT_COUNT" AS BIGINT) ELSE 0 END) AS "TOTALCOMMENTCOUNT", SUM(CASE WHEN "t5"."UPVOTE_COUNT" IS NOT NULL THEN CAST("t5"."UPVOTE_COUNT" AS INTEGER) ELSE 0 END) AS "TOTALUPVOTES", SUM(CASE WHEN "t5"."DOWNVOTE_COUNT" IS NOT NULL THEN CAST("t5"."DOWNVOTE_COUNT" AS INTEGER) ELSE 0 END) AS "TOTALDOWNVOTES"
FROM (SELECT "t1"."Id", "t1"."Reputation", "t1"."CreationDate", "t1"."DisplayName", "t1"."LastAccessDate", "t1"."WebsiteUrl", "t1"."Location", "t1"."AboutMe", "t1"."Views", "t1"."UpVotes", "t1"."DownVotes", "t1"."ProfileImageUrl", "t1"."AccountId", "t2"."Id" AS "Id0", "t2"."PostTypeId", "t2"."AcceptedAnswerId", "t2"."ParentId", "t2"."CreationDate" AS "CreationDate0", "t2"."Score", "t2"."ViewCount", "t2"."Body", "t2"."OwnerUserId", "t2"."OwnerDisplayName", "t2"."LastEditorUserId", "t2"."LastEditorDisplayName", "t2"."LastEditDate", "t2"."LastActivityDate", "t2"."Title", "t2"."Tags", "t2"."AnswerCount", "t2"."CommentCount", "t2"."FavoriteCount", "t2"."ClosedDate", "t2"."CommunityOwnedDate", "t2"."ContentLicense", "Posts"."Id" AS "Id1", "Posts"."PostTypeId" AS "PostTypeId0", "Posts"."AcceptedAnswerId" AS "AcceptedAnswerId0", "Posts"."ParentId" AS "ParentId0", "Posts"."CreationDate" AS "CreationDate1", "Posts"."Score" AS "Score0", "Posts"."ViewCount" AS "ViewCount0", "Posts"."Body" AS "Body0", "Posts"."OwnerUserId" AS "OwnerUserId0", "Posts"."OwnerDisplayName" AS "OwnerDisplayName0", "Posts"."LastEditorUserId" AS "LastEditorUserId0", "Posts"."LastEditorDisplayName" AS "LastEditorDisplayName0", "Posts"."LastEditDate" AS "LastEditDate0", "Posts"."LastActivityDate" AS "LastActivityDate0", "Posts"."Title" AS "Title0", "Posts"."Tags" AS "Tags0", "Posts"."AnswerCount" AS "AnswerCount0", "Posts"."CommentCount" AS "CommentCount0", "Posts"."FavoriteCount" AS "FavoriteCount0", "Posts"."ClosedDate" AS "ClosedDate0", "Posts"."CommunityOwnedDate" AS "CommunityOwnedDate0", "Posts"."ContentLicense" AS "ContentLicense0"
FROM "STACK"."Posts"
RIGHT JOIN ((SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t1" LEFT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t2" ON "t1"."Id" = "t2"."OwnerUserId") ON "Posts"."ParentId" = "t2"."Id") AS "t3"
LEFT JOIN (SELECT "PostId", COUNT(*) AS "COMMENT_COUNT"
FROM "STACK"."Comments"
GROUP BY "PostId") AS "t4" ON "t3"."Id0" = "t4"."PostId"
LEFT JOIN (SELECT "POSTID", SUM("FD_COL_1") AS "UPVOTE_COUNT", SUM("FD_COL_2") AS "DOWNVOTE_COUNT"
FROM "s1"
GROUP BY "POSTID") AS "t5" ON "t3"."Id0" = "t5"."POSTID"
GROUP BY "t3"."Id", "t3"."DisplayName", "t3"."Reputation"
HAVING COUNT(DISTINCT "t3"."Id0") > 0
ORDER BY "t3"."Reputation" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t10"