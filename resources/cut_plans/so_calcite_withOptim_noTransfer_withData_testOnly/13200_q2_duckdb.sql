SELECT COALESCE("t2"."USERID", "t2"."USERID") AS "USERID", "t2"."POSTCOUNT", "t2"."TOTALUPVOTES", "t2"."TOTALDOWNVOTES", "t6"."POSTID", "t6"."POSTTYPEID", "t6"."COMMENTCOUNT", "t6"."VOTECOUNT", "t6"."UPVOTECOUNT", "t6"."DOWNVOTECOUNT"
FROM (SELECT ANY_VALUE("Users"."Id") AS "USERID", COUNT(DISTINCT "Posts0"."Id") AS "POSTCOUNT", SUM("Users"."UpVotes") AS "TOTALUPVOTES", SUM("Users"."DownVotes") AS "TOTALDOWNVOTES"
FROM "STACK"."Users"
INNER JOIN "STACK"."Posts" AS "Posts0" ON "Users"."Id" = "Posts0"."OwnerUserId"
GROUP BY "Users"."Id") AS "t2"
INNER JOIN (SELECT ANY_VALUE("t3"."Id") AS "POSTID", "t3"."PostTypeId" AS "POSTTYPEID", COUNT("t3"."Id0") AS "COMMENTCOUNT", COUNT("Votes"."Id") AS "VOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT"
FROM "STACK"."Votes"
RIGHT JOIN (SELECT "s1"."Id", "s1"."PostTypeId", "s1"."AcceptedAnswerId", "s1"."ParentId", "s1"."CreationDate", "s1"."Score", "s1"."ViewCount", "s1"."Body", "s1"."OwnerUserId", "s1"."OwnerDisplayName", "s1"."LastEditorUserId", "s1"."LastEditorDisplayName", "s1"."LastEditDate", "s1"."LastActivityDate", "s1"."Title", "s1"."Tags", "s1"."AnswerCount", "s1"."CommentCount", "s1"."FavoriteCount", "s1"."ClosedDate", "s1"."CommunityOwnedDate", "s1"."ContentLicense", "Comments"."Id" AS "Id0", "Comments"."PostId", "Comments"."Score" AS "Score0", "Comments"."Text", "Comments"."CreationDate" AS "CreationDate0", "Comments"."UserDisplayName", "Comments"."UserId", "Comments"."ContentLicense" AS "ContentLicense0"
FROM "STACK"."Comments"
RIGHT JOIN "s1" ON "Comments"."PostId" = "s1"."Id") AS "t3" ON "Votes"."PostId" = "t3"."Id"
GROUP BY "t3"."Id", "t3"."PostTypeId") AS "t6" ON "t2"."USERID" = "t6"."POSTID"
ORDER BY "t2"."POSTCOUNT" DESC NULLS FIRST, "t6"."VOTECOUNT" DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY