SELECT COALESCE("TITLE", "TITLE") AS "TITLE", "NAME", "PERSON_ROLE_ID", "KIND"
FROM (SELECT "title" AS "TITLE", "name" AS "NAME", "person_role_id" AS "PERSON_ROLE_ID", "kind" AS "KIND"
FROM "s1"
ORDER BY "title") AS "t2"