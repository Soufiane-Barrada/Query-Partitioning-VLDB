SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "CAST_TYPE", "PERSON_INFO", "MOVIE_KEYWORD", "production_year"
FROM (SELECT "name" AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "kind" AS "CAST_TYPE", "info" AS "PERSON_INFO", "keyword" AS "MOVIE_KEYWORD", "production_year"
FROM "s1"
ORDER BY "name", "production_year" DESC NULLS FIRST) AS "t5"