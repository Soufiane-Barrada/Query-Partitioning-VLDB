SELECT COALESCE("t12"."ACTIVEUSER", "t12"."ACTIVEUSER") AS "ACTIVEUSER", "t12"."POSTSCOUNT", "t12"."TOTALCOMMENTS", "t12"."TOTALUPVOTES", "t12"."TOTALDOWNVOTES", "t12"."TAGNAME", "t12"."POSTCOUNT"
FROM (SELECT "t10"."DISPLAYNAME" AS "ACTIVEUSER", "t10"."POSTSCOUNT", "t10"."TOTALCOMMENTS", "t10"."TOTALUPVOTES", "t10"."TOTALDOWNVOTES", "t3"."TAGNAME", "t3"."POSTCOUNT"
FROM (SELECT "TAGID", "TagName" AS "TAGNAME", "POSTCOUNT"
FROM (SELECT "Id0", "TagName", ANY_VALUE("Id0") AS "TAGID", COUNT("Id") AS "POSTCOUNT"
FROM "s1"
GROUP BY "Id0", "TagName"
HAVING COUNT("Id") > 0
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2") AS "t3",
(SELECT ANY_VALUE("t5"."Id") AS "USERID", "t5"."DisplayName" AS "DISPLAYNAME", COUNT("t5"."Id0") AS "POSTSCOUNT", SUM(CASE WHEN "t6"."COMMENTSCOUNT" IS NOT NULL THEN CAST("t6"."COMMENTSCOUNT" AS BIGINT) ELSE 0 END) AS "TOTALCOMMENTS", SUM("t5"."UpVotes") AS "TOTALUPVOTES", SUM("t5"."DownVotes") AS "TOTALDOWNVOTES", COUNT("t5"."Id0") > 5 AS "$f6"
FROM (SELECT "t4"."Id", "t4"."Reputation", "t4"."CreationDate", "t4"."DisplayName", "t4"."LastAccessDate", "t4"."WebsiteUrl", "t4"."Location", "t4"."AboutMe", "t4"."Views", "t4"."UpVotes", "t4"."DownVotes", "t4"."ProfileImageUrl", "t4"."AccountId", "Posts0"."Id" AS "Id0", "Posts0"."PostTypeId", "Posts0"."AcceptedAnswerId", "Posts0"."ParentId", "Posts0"."CreationDate" AS "CreationDate0", "Posts0"."Score", "Posts0"."ViewCount", "Posts0"."Body", "Posts0"."OwnerUserId", "Posts0"."OwnerDisplayName", "Posts0"."LastEditorUserId", "Posts0"."LastEditorDisplayName", "Posts0"."LastEditDate", "Posts0"."LastActivityDate", "Posts0"."Title", "Posts0"."Tags", "Posts0"."AnswerCount", "Posts0"."CommentCount", "Posts0"."FavoriteCount", "Posts0"."ClosedDate", "Posts0"."CommunityOwnedDate", "Posts0"."ContentLicense"
FROM "STACK"."Posts" AS "Posts0"
RIGHT JOIN (SELECT *
FROM "STACK"."Users"
WHERE "Reputation" > 1000) AS "t4" ON "Posts0"."OwnerUserId" = "t4"."Id") AS "t5"
LEFT JOIN (SELECT "PostId", COUNT(*) AS "COMMENTSCOUNT"
FROM "STACK"."Comments"
GROUP BY "PostId") AS "t6" ON "t5"."Id0" = "t6"."PostId"
GROUP BY "t5"."Id", "t5"."DisplayName"
HAVING COUNT("t5"."Id0") > 5) AS "t10"
ORDER BY "t10"."TOTALUPVOTES" DESC NULLS FIRST, "t3"."POSTCOUNT" DESC NULLS FIRST) AS "t12"