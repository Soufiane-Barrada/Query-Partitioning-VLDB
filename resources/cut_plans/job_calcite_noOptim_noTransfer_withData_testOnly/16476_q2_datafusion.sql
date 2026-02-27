SELECT COALESCE("title", "title") AS "TITLE", "name0" AS "PERSON_NAME", "role" AS "ROLE", "production_year"
FROM "s1"
WHERE "country_code" = 'USA'
ORDER BY "production_year" DESC NULLS FIRST