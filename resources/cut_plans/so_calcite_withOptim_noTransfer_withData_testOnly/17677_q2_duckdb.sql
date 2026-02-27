SELECT COALESCE("t2"."ID", "t2"."ID") AS "ID", "t2"."TITLE", "t2"."CREATIONDATE", "t2"."SCORE", "t2"."OWNERDISPLAYNAME"
FROM (SELECT "s1"."Id" AS "ID", "s1"."Title" AS "TITLE", "s1"."CreationDate" AS "CREATIONDATE", "s1"."Score" AS "SCORE", "Users"."DisplayName" AS "OWNERDISPLAYNAME"
FROM "s1"
INNER JOIN "STACK"."Users" ON "s1"."OwnerUserId" = "Users"."Id"
ORDER BY "s1"."CreationDate" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"