SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "CAST_TYPE", "COMPANY_NAME", "MOVIE_INFO", "MOVIE_KEYWORD", "production_year"
FROM (SELECT "name0" AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "kind" AS "CAST_TYPE", "name" AS "COMPANY_NAME", "info" AS "MOVIE_INFO", "keyword" AS "MOVIE_KEYWORD", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST, "name0") AS "t5"