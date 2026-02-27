SELECT COALESCE("t0"."Id", "t0"."Id") AS "Id", "t0"."Title" AS "TITLE", "t0"."CreationDate" AS "CREATIONDATE", "t0"."Id0", "t1"."BountyAmount", "Tags"."TagName", ', ' AS "FD_COL_6"
FROM (SELECT "t"."Id", "t"."PostTypeId", "t"."AcceptedAnswerId", "t"."ParentId", "t"."CreationDate", "t"."Score", "t"."ViewCount", "t"."Body", "t"."OwnerUserId", "t"."OwnerDisplayName", "t"."LastEditorUserId", "t"."LastEditorDisplayName", "t"."LastEditDate", "t"."LastActivityDate", "t"."Title", "t"."Tags", "t"."AnswerCount", "t"."CommentCount", "t"."FavoriteCount", "t"."ClosedDate", "t"."CommunityOwnedDate", "t"."ContentLicense", "Comments"."Id" AS "Id0", "Comments"."PostId", "Comments"."Score" AS "Score0", "Comments"."Text", "Comments"."CreationDate" AS "CreationDate0", "Comments"."UserDisplayName", "Comments"."UserId", "Comments"."ContentLicense" AS "ContentLicense0"
FROM "STACK"."Comments"
RIGHT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 1) AS "t" ON "Comments"."PostId" = "t"."Id") AS "t0"
LEFT JOIN (SELECT *
FROM "STACK"."Votes"
WHERE CAST("VoteTypeId" AS INTEGER) = 9) AS "t1" ON "t0"."Id" = "t1"."PostId"
LEFT JOIN "STACK"."Tags" ON "t0"."Id" = "Tags"."ExcerptPostId" OR "t0"."Id" = "Tags"."WikiPostId"