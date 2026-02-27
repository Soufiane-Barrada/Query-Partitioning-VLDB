SELECT COALESCE("AKA_NAME", "AKA_NAME") AS "AKA_NAME", "MOVIE_TITLE", "CAST_NOTE", "CAST_TYPE", "COMPANY_NAME", "MOVIE_INFO", "MOVIE_KEYWORD", "production_year"
FROM (SELECT "name" AS "AKA_NAME", "title" AS "MOVIE_TITLE", "note0" AS "CAST_NOTE", "kind" AS "CAST_TYPE", "name0" AS "COMPANY_NAME", "info" AS "MOVIE_INFO", "keyword" AS "MOVIE_KEYWORD", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST, "name") AS "t3"