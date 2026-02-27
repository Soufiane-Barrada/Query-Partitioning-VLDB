SELECT COALESCE("title", "title") AS "TITLE", "name0" AS "NAME", "person_role_id" AS "PERSON_ROLE_ID", "kind" AS "KIND"
FROM "s1"
WHERE "production_year" = 2020
ORDER BY "title"