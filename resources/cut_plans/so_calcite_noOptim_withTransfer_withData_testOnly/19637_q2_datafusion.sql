SELECT COALESCE("s1"."Title", "s1"."Title") AS "TITLE", "s1"."CreationDate" AS "CREATIONDATE", "s1"."DisplayName" AS "DISPLAYNAME", "Tags"."TagName" AS "TAGNAME", "s1"."Score"
FROM "s1"
INNER JOIN "STACK"."Tags" ON "s1"."Tags" LIKE CONCAT('%', "Tags"."TagName", '%')
WHERE CAST("s1"."PostTypeId" AS INTEGER) = 1
ORDER BY "s1"."Score" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY