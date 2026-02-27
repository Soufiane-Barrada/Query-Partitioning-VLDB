SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "ROLE_ORDER", "MOVIE_KEYWORD", "ROLE_TYPE", "production_year"
FROM (SELECT "name" AS "ACTOR_NAME", "title" AS "MOVIE_TITLE", "nr_order" AS "ROLE_ORDER", "keyword" AS "MOVIE_KEYWORD", "role" AS "ROLE_TYPE", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST, "nr_order") AS "t2"