SELECT COALESCE("t9"."POSTID", "t9"."POSTID") AS "POSTID", "t9"."TITLE", "t9"."CREATIONDATE", "t9"."OWNERDISPLAYNAME", "t9"."NUMBEROFCOMMENTS", "t9"."NUMBEROFANSWERS", "t9"."TOTALUPVOTES"
FROM (SELECT "t7"."Id" AS "POSTID", "t7"."Title" AS "TITLE", "t7"."CreationDate" AS "CREATIONDATE", "t7"."DisplayName" AS "OWNERDISPLAYNAME", CASE WHEN "t7"."COMMENTCOUNT" IS NOT NULL THEN CAST("t7"."COMMENTCOUNT" AS BIGINT) ELSE 0 END AS "NUMBEROFCOMMENTS", CASE WHEN "t7"."ANSWERCOUNT" IS NOT NULL THEN CAST("t7"."ANSWERCOUNT" AS BIGINT) ELSE 0 END AS "NUMBEROFANSWERS", CASE WHEN "t4"."TOTALUPVOTES" IS NOT NULL THEN CAST("t4"."TOTALUPVOTES" AS BIGINT) ELSE 0 END AS "TOTALUPVOTES"
FROM (SELECT "PostId", COUNT(*) AS "TOTALUPVOTES"
FROM "STACK"."Votes"
WHERE CAST("VoteTypeId" AS INTEGER) = 2
GROUP BY "PostId") AS "t4"
RIGHT JOIN (SELECT "s1"."Id", "s1"."PostTypeId", "s1"."AcceptedAnswerId", "s1"."ParentId", "s1"."CreationDate", "s1"."Score", "s1"."ViewCount", "s1"."Body", "s1"."OwnerUserId", "s1"."OwnerDisplayName", "s1"."LastEditorUserId", "s1"."LastEditorDisplayName", "s1"."LastEditDate", "s1"."LastActivityDate", "s1"."Title", "s1"."Tags", "s1"."AnswerCount", "s1"."CommentCount", "s1"."FavoriteCount", "s1"."ClosedDate", "s1"."CommunityOwnedDate", "s1"."ContentLicense", "s1"."Id0", "s1"."Reputation", "s1"."CreationDate0", "s1"."DisplayName", "s1"."LastAccessDate", "s1"."WebsiteUrl", "s1"."Location", "s1"."AboutMe", "s1"."Views", "s1"."UpVotes", "s1"."DownVotes", "s1"."ProfileImageUrl", "s1"."AccountId", "s1"."POSTID", "s1"."COMMENTCOUNT_" AS "COMMENTCOUNT", "t6"."ParentId" AS "PARENTID", "t6"."ANSWERCOUNT"
FROM (SELECT "ParentId", COUNT(*) AS "ANSWERCOUNT"
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 2
GROUP BY "ParentId") AS "t6"
RIGHT JOIN "s1" ON "t6"."ParentId" = "s1"."Id") AS "t7" ON "t4"."PostId" = "t7"."Id"
ORDER BY "t7"."CreationDate" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t9"