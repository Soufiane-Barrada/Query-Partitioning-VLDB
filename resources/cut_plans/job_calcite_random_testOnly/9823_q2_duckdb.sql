SELECT COALESCE("AKA_NAME", "AKA_NAME") AS "AKA_NAME", "MOVIE_TITLE", "CAST_NOTE", "PERSON_INFO", "MOVIE_KEYWORD", "COMPANY_NAME", "ROLE_TYPE", "production_year"
FROM (SELECT "name0" AS "AKA_NAME", "title" AS "MOVIE_TITLE", "note" AS "CAST_NOTE", "info" AS "PERSON_INFO", "keyword" AS "MOVIE_KEYWORD", "name" AS "COMPANY_NAME", "role" AS "ROLE_TYPE", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST, "name0") AS "t4"