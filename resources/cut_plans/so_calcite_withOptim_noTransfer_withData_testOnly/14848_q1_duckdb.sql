SELECT COALESCE("t0"."POSTTYPEID", "t0"."POSTTYPEID") AS "POSTTYPEID", "t0"."TOTALPOSTS", "t0"."TOTALACCEPTEDANSWERS", "t5"."USERID", "t5"."REPUTATION", "t5"."POSTSCOUNT", "t5"."TOTALBOUNTIES", "t5"."$f4" AS "FD_COL_7"
FROM (SELECT "PostTypeId" AS "POSTTYPEID", COUNT(*) AS "TOTALPOSTS", SUM(1) AS "TOTALACCEPTEDANSWERS"
FROM "STACK"."Posts"
GROUP BY "PostTypeId") AS "t0",
(SELECT ANY_VALUE("t1"."Id") AS "USERID", "t1"."Reputation" AS "REPUTATION", COUNT(DISTINCT "t1"."Id0") AS "POSTSCOUNT", SUM("t2"."BountyAmount") AS "TOTALBOUNTIES", COUNT(DISTINCT "t1"."Id0") > 0 AS "$f4"
FROM (SELECT "Users"."Id", "Users"."Reputation", "Users"."CreationDate", "Users"."DisplayName", "Users"."LastAccessDate", "Users"."WebsiteUrl", "Users"."Location", "Users"."AboutMe", "Users"."Views", "Users"."UpVotes", "Users"."DownVotes", "Users"."ProfileImageUrl", "Users"."AccountId", "Posts0"."Id" AS "Id0", "Posts0"."PostTypeId", "Posts0"."AcceptedAnswerId", "Posts0"."ParentId", "Posts0"."CreationDate" AS "CreationDate0", "Posts0"."Score", "Posts0"."ViewCount", "Posts0"."Body", "Posts0"."OwnerUserId", "Posts0"."OwnerDisplayName", "Posts0"."LastEditorUserId", "Posts0"."LastEditorDisplayName", "Posts0"."LastEditDate", "Posts0"."LastActivityDate", "Posts0"."Title", "Posts0"."Tags", "Posts0"."AnswerCount", "Posts0"."CommentCount", "Posts0"."FavoriteCount", "Posts0"."ClosedDate", "Posts0"."CommunityOwnedDate", "Posts0"."ContentLicense"
FROM "STACK"."Posts" AS "Posts0"
RIGHT JOIN "STACK"."Users" ON "Posts0"."OwnerUserId" = "Users"."Id") AS "t1"
LEFT JOIN (SELECT *
FROM "STACK"."Votes"
WHERE CAST("VoteTypeId" AS INTEGER) = 8) AS "t2" ON "t1"."Id0" = "t2"."PostId"
GROUP BY "t1"."Id", "t1"."Reputation"
HAVING COUNT(DISTINCT "t1"."Id0") > 0) AS "t5"