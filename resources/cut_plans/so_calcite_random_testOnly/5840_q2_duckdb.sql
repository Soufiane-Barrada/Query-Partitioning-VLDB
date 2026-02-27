SELECT COALESCE("t6"."USER", "t6"."USER") AS "USER", "t6"."POST_TITLE", "t6"."POST_CREATION_DATE", "t6"."POST_SCORE", "t6"."AVERAGE_COMMENT_SCORE", "t6"."TOTAL_UPVOTES", "t6"."TOTAL_DOWNVOTES"
FROM (SELECT "t4"."DisplayName" AS "USER", "t4"."Title" AS "POST_TITLE", "t4"."CreationDate" AS "POST_CREATION_DATE", "t4"."Score" AS "POST_SCORE", CASE WHEN "t4"."AVG_COMMENT_SCORE" IS NOT NULL THEN CAST("t4"."AVG_COMMENT_SCORE" AS INTEGER) ELSE 0 END AS "AVERAGE_COMMENT_SCORE", CASE WHEN "t2"."UPVOTES" IS NOT NULL THEN CAST("t2"."UPVOTES" AS INTEGER) ELSE 0 END AS "TOTAL_UPVOTES", CASE WHEN "t2"."DOWNVOTES" IS NOT NULL THEN CAST("t2"."DOWNVOTES" AS INTEGER) ELSE 0 END AS "TOTAL_DOWNVOTES"
FROM (SELECT "PostId" AS "POSTID", SUM(CASE WHEN CAST("VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES"
FROM "STACK"."Votes"
GROUP BY "PostId") AS "t2"
RIGHT JOIN (SELECT "t3"."Id", "t3"."PostTypeId", "t3"."AcceptedAnswerId", "t3"."ParentId", "t3"."CreationDate", "t3"."Score", "t3"."ViewCount", "t3"."Body", "t3"."OwnerUserId", "t3"."OwnerDisplayName", "t3"."LastEditorUserId", "t3"."LastEditorDisplayName", "t3"."LastEditDate", "t3"."LastActivityDate", "t3"."Title", "t3"."Tags", "t3"."AnswerCount", "t3"."CommentCount", "t3"."FavoriteCount", "t3"."ClosedDate", "t3"."CommunityOwnedDate", "t3"."ContentLicense", "Users"."Id" AS "Id0", "Users"."Reputation", "Users"."CreationDate" AS "CreationDate0", "Users"."DisplayName", "Users"."LastAccessDate", "Users"."WebsiteUrl", "Users"."Location", "Users"."AboutMe", "Users"."Views", "Users"."UpVotes", "Users"."DownVotes", "Users"."ProfileImageUrl", "Users"."AccountId", "s1"."PostId" AS "POSTID", "s1"."AVG_COMMENT_SCORE"
FROM "s1"
RIGHT JOIN ("STACK"."Users" INNER JOIN (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t3" ON "Users"."Id" = "t3"."OwnerUserId") ON "s1"."PostId" = "t3"."Id") AS "t4" ON "t2"."POSTID" = "t4"."Id"
ORDER BY "t4"."CreationDate" DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t6"