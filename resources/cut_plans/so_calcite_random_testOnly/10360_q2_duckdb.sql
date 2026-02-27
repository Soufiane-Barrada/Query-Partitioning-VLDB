SELECT COALESCE("t6"."POSTID", "t6"."POSTID") AS "POSTID", "t6"."TITLE", "t6"."CREATIONDATE", "t6"."COMMENTCOUNT", "t6"."VOTECOUNT", "t6"."UPVOTECOUNT", "t6"."DOWNVOTECOUNT", "t9"."USERID", "t9"."DISPLAYNAME", "t9"."POSTSCOUNT", "t9"."BADGESCOUNT", "t9"."TOTALUPVOTES", "t9"."TOTALDOWNVOTES"
FROM (SELECT ANY_VALUE("t3"."Id") AS "POSTID", "t3"."Title" AS "TITLE", "t3"."CreationDate" AS "CREATIONDATE", COUNT(DISTINCT "t3"."Id0") AS "COMMENTCOUNT", COUNT(DISTINCT "Votes"."Id") AS "VOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT"
FROM "STACK"."Votes"
RIGHT JOIN (SELECT "t2"."Id", "t2"."PostTypeId", "t2"."AcceptedAnswerId", "t2"."ParentId", "t2"."CreationDate", "t2"."Score", "t2"."ViewCount", "t2"."Body", "t2"."OwnerUserId", "t2"."OwnerDisplayName", "t2"."LastEditorUserId", "t2"."LastEditorDisplayName", "t2"."LastEditDate", "t2"."LastActivityDate", "t2"."Title", "t2"."Tags", "t2"."AnswerCount", "t2"."CommentCount", "t2"."FavoriteCount", "t2"."ClosedDate", "t2"."CommunityOwnedDate", "t2"."ContentLicense", "Comments"."Id" AS "Id0", "Comments"."PostId", "Comments"."Score" AS "Score0", "Comments"."Text", "Comments"."CreationDate" AS "CreationDate0", "Comments"."UserDisplayName", "Comments"."UserId", "Comments"."ContentLicense" AS "ContentLicense0"
FROM "STACK"."Comments"
RIGHT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= CAST((CURRENT_DATE - INTERVAL '1' YEAR) AS TIMESTAMP(0))) AS "t2" ON "Comments"."PostId" = "t2"."Id") AS "t3" ON "Votes"."PostId" = "t3"."Id"
GROUP BY "t3"."Id", "t3"."Title", "t3"."CreationDate") AS "t6"
INNER JOIN (SELECT ANY_VALUE("s1"."Id") AS "USERID", "s1"."DisplayName" AS "DISPLAYNAME", COUNT(DISTINCT "s1"."Id0") AS "POSTSCOUNT", COUNT(DISTINCT "Badges"."Id") AS "BADGESCOUNT", SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "TOTALUPVOTES", SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "TOTALDOWNVOTES"
FROM "STACK"."Votes" AS "Votes0"
RIGHT JOIN ("s1" LEFT JOIN "STACK"."Badges" ON "s1"."Id" = "Badges"."UserId") ON "Votes0"."PostId" = "s1"."Id0"
GROUP BY "s1"."Id", "s1"."DisplayName") AS "t9" ON "t6"."POSTID" = "t9"."USERID"
ORDER BY "t6"."CREATIONDATE" DESC NULLS FIRST