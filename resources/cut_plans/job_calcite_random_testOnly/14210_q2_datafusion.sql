SELECT COALESCE("AKA_NAME", "AKA_NAME") AS "AKA_NAME", "MOVIE_TITLE", "PERSON_NAME", "PERSON_ROLE", "CAST_NOTE", "COMPANY_NAME", "MOVIE_INFO", "production_year"
FROM (SELECT "name" AS "AKA_NAME", "title" AS "MOVIE_TITLE", "name0" AS "PERSON_NAME", "role" AS "PERSON_ROLE", "note" AS "CAST_NOTE", "name1" AS "COMPANY_NAME", "info" AS "MOVIE_INFO", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST, "name") AS "t2"