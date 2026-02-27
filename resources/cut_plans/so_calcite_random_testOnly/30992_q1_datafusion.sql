SELECT COALESCE("t1"."Id", "t1"."Id") AS "Id", "t1"."PostTypeId", "t1"."AcceptedAnswerId", "t1"."ParentId", "t1"."CreationDate", "t1"."Score", "t1"."ViewCount", "t1"."Body", "t1"."OwnerUserId", "t1"."OwnerDisplayName", "t1"."LastEditorUserId", "t1"."LastEditorDisplayName", "t1"."LastEditDate", "t1"."LastActivityDate", "t1"."Title", "t1"."Tags", "t1"."AnswerCount", "t1"."CommentCount", "t1"."FavoriteCount", "t1"."ClosedDate", "t1"."CommunityOwnedDate", "t1"."ContentLicense", "t0"."POSTID", "t0"."UPVOTES", "t0"."DOWNVOTES", "t0"."TOTALVOTES"
FROM (SELECT "PostId" AS "POSTID", SUM(CASE WHEN CAST("VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTES", SUM(CASE WHEN CAST("VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTES", COUNT(*) AS "TOTALVOTES"
FROM "STACK"."Votes"
GROUP BY "PostId") AS "t0"
RIGHT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '30' DAY)) AS "t1" ON "t0"."POSTID" = "t1"."Id"