SELECT COALESCE("s1"."Id", "s1"."Id") AS "ID", "s1"."Title" AS "TITLE", "s1"."CreationDate" AS "CREATIONDATE", "s1"."DisplayName" AS "DISPLAYNAME", "Tags"."TagName" AS "TAGNAME"
FROM "s1"
INNER JOIN "STACK"."Tags" ON "s1"."Tags" LIKE CONCAT('%', "Tags"."TagName", '%')
WHERE CAST("s1"."PostTypeId" AS INTEGER) = 1
ORDER BY "s1"."CreationDate" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY