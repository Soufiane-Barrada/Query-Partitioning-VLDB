SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "PRODUCTION_YEAR", "ROLE_ID", "TOTAL_MOVIES", "name"
FROM (SELECT "ACTOR_NAME", "MOVIE_TITLE", "production_year" AS "PRODUCTION_YEAR", "role_id" AS "ROLE_ID", "TOTAL_MOVIES", "name"
FROM (SELECT "aka_name"."name", "t1"."title", "t1"."production_year", "cast_info"."role_id", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t1"."title") AS "MOVIE_TITLE", COUNT(*) AS "TOTAL_MOVIES"
FROM "IMDB"."role_type"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "role_type"."id" = "cast_info"."role_id"
INNER JOIN ("s1" INNER JOIN "IMDB"."movie_companies" ON "s1"."id" = "movie_companies"."company_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."complete_cast" ON "t1"."id" = "complete_cast"."movie_id") ON "movie_companies"."movie_id" = "t1"."id") ON "cast_info"."movie_id" = "t1"."movie_id"
GROUP BY "aka_name"."name", "cast_info"."role_id", "t1"."title", "t1"."production_year") AS "t3"
WHERE "t3"."TOTAL_MOVIES" > 1
ORDER BY "TOTAL_MOVIES" DESC NULLS FIRST, "name") AS "t6"