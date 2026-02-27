SELECT COALESCE("t2"."DISPLAYNAME", "t2"."DISPLAYNAME") AS "DISPLAYNAME", "t2"."TITLE", "t2"."CREATIONDATE", "t2"."SCORE", "t2"."TAGNAME"
FROM (SELECT "Users"."DisplayName" AS "DISPLAYNAME", "s1"."Title" AS "TITLE", "s1"."CreationDate" AS "CREATIONDATE", "s1"."Score" AS "SCORE", "Tags"."TagName" AS "TAGNAME"
FROM "STACK"."Users"
INNER JOIN ("STACK"."Tags" INNER JOIN "s1" ON "s1"."Tags" LIKE '%' || "Tags"."TagName" || '%') ON "Users"."Id" = "s1"."OwnerUserId"
ORDER BY "s1"."CreationDate" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"