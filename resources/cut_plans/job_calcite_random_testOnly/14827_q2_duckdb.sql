SELECT COALESCE("AKA_NAME", "AKA_NAME") AS "AKA_NAME", "MOVIE_TITLE", "CAST_ORDER", "ACTOR_NAME", "ACTOR_INFO", "production_year"
FROM (SELECT "name0" AS "AKA_NAME", "title" AS "MOVIE_TITLE", "nr_order" AS "CAST_ORDER", "name" AS "ACTOR_NAME", "info" AS "ACTOR_INFO", "production_year"
FROM "s1"
ORDER BY "production_year" DESC NULLS FIRST, "nr_order") AS "t2"