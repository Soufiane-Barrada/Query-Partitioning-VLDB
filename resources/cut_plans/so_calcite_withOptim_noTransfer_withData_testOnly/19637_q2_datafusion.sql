SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."CREATIONDATE", "t2"."DISPLAYNAME", "t2"."TAGNAME", "t2"."Score"
FROM (SELECT "s1"."Title" AS "TITLE", "s1"."CreationDate" AS "CREATIONDATE", "Users"."DisplayName" AS "DISPLAYNAME", "s1"."TagName" AS "TAGNAME", "s1"."Score"
FROM "STACK"."Users"
INNER JOIN "s1" ON "Users"."Id" = "s1"."OwnerUserId"
ORDER BY "s1"."Score" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"