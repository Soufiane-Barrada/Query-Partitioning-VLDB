SELECT COALESCE("t"."DISPLAYNAME", "t"."DISPLAYNAME") AS "DISPLAYNAME", "t"."REPUTATION", "t3"."TITLE", "t3"."CREATIONDATE", "t3"."COMMENTCOUNT", "t3"."UPVOTECOUNT", "t3"."DOWNVOTECOUNT", "t7"."TOTALUPVOTES" AS "ACCEPTEDANSWERUPVOTES", "t9"."RELATEDPOSTCOUNT", "t9"."DUPLICATECOUNT"
FROM (SELECT "Id" AS "ID", "DisplayName" AS "DISPLAYNAME", "Reputation" AS "REPUTATION", ROW_NUMBER() OVER (ORDER BY "Reputation" DESC NULLS FIRST) AS "RAN"
FROM "STACK"."Users") AS "t"
INNER JOIN (SELECT ANY_VALUE("Posts"."Id") AS "POSTID", "Posts"."Title" AS "TITLE", "Posts"."CreationDate" AS "CREATIONDATE", "Posts"."OwnerUserId" AS "OWNERUSERID", COUNT("Comments"."Id") AS "COMMENTCOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "UPVOTECOUNT", SUM(CASE WHEN CAST("Votes"."VoteTypeId" AS INTEGER) = 3 THEN 1 ELSE 0 END) AS "DOWNVOTECOUNT"
FROM "STACK"."Posts"
LEFT JOIN "STACK"."Comments" ON "Posts"."Id" = "Comments"."PostId"
LEFT JOIN "STACK"."Votes" ON "Posts"."Id" = "Votes"."PostId"
WHERE "Posts"."CreationDate" >= (CAST('2024-10-01 12:34:56' AS TIMESTAMP(0)) - INTERVAL '30' DAY)
GROUP BY "Posts"."Id", "Posts"."Title", "Posts"."CreationDate", "Posts"."OwnerUserId") AS "t3" ON "t"."ID" = "t3"."OWNERUSERID"
LEFT JOIN (SELECT ANY_VALUE("Posts0"."Id") AS "ANSWERID", "Posts0"."AcceptedAnswerId" AS "ACCEPTEDANSWERID", SUM(CASE WHEN CAST("Votes0"."VoteTypeId" AS INTEGER) = 2 THEN 1 ELSE 0 END) AS "TOTALUPVOTES"
FROM "STACK"."Posts" AS "Posts0"
LEFT JOIN "STACK"."Votes" AS "Votes0" ON "Posts0"."Id" = "Votes0"."PostId"
WHERE CAST("Posts0"."PostTypeId" AS INTEGER) = 2
GROUP BY "Posts0"."Id", "Posts0"."AcceptedAnswerId") AS "t7" ON "t3"."POSTID" = "t7"."ACCEPTEDANSWERID"
LEFT JOIN (SELECT "PostLinks"."PostId" AS "POSTID", COUNT(DISTINCT "PostLinks"."RelatedPostId") AS "RELATEDPOSTCOUNT", SUM(CASE WHEN "LinkTypes"."Name" = 'Duplicate' THEN 1 ELSE 0 END) AS "DUPLICATECOUNT"
FROM "STACK"."PostLinks"
INNER JOIN "STACK"."LinkTypes" ON "PostLinks"."LinkTypeId" = "LinkTypes"."Id"
GROUP BY "PostLinks"."PostId") AS "t9" ON "t3"."POSTID" = "t9"."POSTID"
WHERE "t"."REPUTATION" > 1000 AND "t3"."UPVOTECOUNT" - "t3"."DOWNVOTECOUNT" > 10