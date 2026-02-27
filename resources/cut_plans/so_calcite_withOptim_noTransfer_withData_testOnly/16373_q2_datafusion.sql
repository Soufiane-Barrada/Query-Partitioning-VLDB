SELECT COALESCE("t2"."ID", "t2"."ID") AS "ID", "t2"."TITLE", "t2"."CREATIONDATE", "t2"."DISPLAYNAME", "t2"."TAGNAME"
FROM (SELECT "s1"."Id0" AS "ID", "s1"."Title" AS "TITLE", "s1"."CreationDate" AS "CREATIONDATE", "Users"."DisplayName" AS "DISPLAYNAME", "s1"."TagName" AS "TAGNAME"
FROM "STACK"."Users"
INNER JOIN "s1" ON "Users"."Id" = "s1"."OwnerUserId"
ORDER BY "s1"."CreationDate" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"