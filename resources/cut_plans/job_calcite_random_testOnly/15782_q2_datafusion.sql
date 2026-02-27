SELECT COALESCE("AKA_NAME", "AKA_NAME") AS "AKA_NAME", "MOVIE_TITLE", "PERSON_NAME", "COMPANY_TYPE", "production_year"
FROM (SELECT "name" AS "AKA_NAME", "title" AS "MOVIE_TITLE", "name0" AS "PERSON_NAME", "kind" AS "COMPANY_TYPE", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST) AS "t2"