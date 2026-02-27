SELECT COALESCE("name", "name") AS "AKA_NAME", "title" AS "MOVIE_TITLE", "person_role_id" AS "PERSON_ROLE_ID", "kind" AS "KIND", "name0" AS "COMPANY_NAME"
FROM "s1"
WHERE "production_year" = 2023
ORDER BY "name", "title"