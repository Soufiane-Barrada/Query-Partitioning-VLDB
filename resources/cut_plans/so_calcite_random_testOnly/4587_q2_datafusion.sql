SELECT COALESCE("t16"."DISPLAYNAME", "t16"."DISPLAYNAME") AS "DISPLAYNAME", "t16"."REPUTATION", "t16"."TITLE", "t16"."CREATIONDATE", "t16"."COMMENTCOUNT", "t16"."UPVOTECOUNT", "t16"."DOWNVOTECOUNT", "t16"."ACCEPTEDANSWERUPVOTES", "t16"."RELATEDPOSTCOUNT", "t16"."DUPLICATECOUNT"
FROM (SELECT "t14"."DISPLAYNAME", "t14"."REPUTATION", "t14"."TITLE", "t14"."CREATIONDATE", "t14"."COMMENTCOUNT", "t14"."UPVOTECOUNT", "t14"."DOWNVOTECOUNT", "t14"."TOTALUPVOTES" AS "ACCEPTEDANSWERUPVOTES", "s1"."RELATEDPOSTCOUNT", "s1"."DUPLICATECOUNT"
FROM "s1"
RIGHT JOIN (SELECT "t7"."ID", "t7"."DISPLAYNAME", "t7"."REPUTATION", "t7"."RAN", "t13"."POSTID", "t13"."TITLE", "t13"."CREATIONDATE", "t13"."OWNERUSERID", "t13"."COMMENTCOUNT", "t13"."UPVOTECOUNT", "t13"."DOWNVOTECOUNT", "t5"."ANSWERID", "t5"."ACCEPTEDANSWERID", "t5"."TOTALUPVOTES"
FROM (SELECT ANY_VALUE("t2"."Id") AS "ANSWERID", "t2"."AcceptedAnswerId" AS "ACCEPTEDANSWERID", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "TOTALUPVOTES"
FROM "STACK"."Votes"
RIGHT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE CAST("PostTypeId" AS INTEGER) = 2) AS "t2" ON "Votes"."PostId" = "t2"."Id"
GROUP BY "t2"."Id", "t2"."AcceptedAnswerId") AS "t5"
RIGHT JOIN ((SELECT *
FROM (SELECT "Id" AS "ID", "DisplayName" AS "DISPLAYNAME", "Reputation" AS "REPUTATION", ROW_NUMBER() OVER (ORDER BY "Reputation" DESC NULLS FIRST) AS "RAN"
FROM "STACK"."Users") AS "t6"
WHERE "REPUTATION" > 1000) AS "t7" INNER JOIN (SELECT ANY_VALUE("t9"."Id") AS "POSTID", "t9"."Title" AS "TITLE", "t9"."CreationDate" AS "CREATIONDATE", "t9"."OwnerUserId" AS "OWNERUSERID", COUNT("t9"."Id0") AS "COMMENTCOUNT", SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT"
FROM "STACK"."Votes" AS "Votes0"
RIGHT JOIN (SELECT "t8"."Id", "t8"."PostTypeId", "t8"."AcceptedAnswerId", "t8"."ParentId", "t8"."CreationDate", "t8"."Score", "t8"."ViewCount", "t8"."Body", "t8"."OwnerUserId", "t8"."OwnerDisplayName", "t8"."LastEditorUserId", "t8"."LastEditorDisplayName", "t8"."LastEditDate", "t8"."LastActivityDate", "t8"."Title", "t8"."Tags", "t8"."AnswerCount", "t8"."CommentCount", "t8"."FavoriteCount", "t8"."ClosedDate", "t8"."CommunityOwnedDate", "t8"."ContentLicense", "Comments"."Id" AS "Id0", "Comments"."PostId", "Comments"."Score" AS "Score0", "Comments"."Text", "Comments"."CreationDate" AS "CreationDate0", "Comments"."UserDisplayName", "Comments"."UserId", "Comments"."ContentLicense" AS "ContentLicense0"
FROM "STACK"."Comments"
RIGHT JOIN (SELECT *
FROM "STACK"."Posts"
WHERE "CreationDate" >= (TIMESTAMP '2024-10-01 12:34:56' - INTERVAL '30' DAY)) AS "t8" ON "Comments"."PostId" = "t8"."Id") AS "t9" ON "Votes0"."PostId" = "t9"."Id"
GROUP BY "t9"."Id", "t9"."Title", "t9"."CreationDate", "t9"."OwnerUserId"
HAVING SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) - SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) > 10) AS "t13" ON "t7"."ID" = "t13"."OWNERUSERID") ON "t5"."ACCEPTEDANSWERID" = "t13"."POSTID") AS "t14" ON "s1"."POSTID" = "t14"."POSTID"
ORDER BY "t14"."REPUTATION" DESC NULLS FIRST, "t14"."CREATIONDATE" DESC NULLS FIRST
OFFSET 0 ROWS
FETCH NEXT 10 ROWS ONLY) AS "t16"