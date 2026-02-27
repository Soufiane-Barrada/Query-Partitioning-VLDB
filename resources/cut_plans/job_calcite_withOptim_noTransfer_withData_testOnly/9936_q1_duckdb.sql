SELECT COALESCE("name", "name") AS "name", "title", "PRODUCTION_YEAR", "ROLE_ID", "ACTOR_NAME", "MOVIE_TITLE", "TOTAL_MOVIES"
FROM (SELECT "aka_name"."name", "t0"."title", "t0"."production_year", "cast_info"."role_id", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", COUNT(*) AS "TOTAL_MOVIES"
FROM "IMDB"."role_type"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "role_type"."id" = "cast_info"."role_id"
INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t" INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."company_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."complete_cast" ON "t0"."id" = "complete_cast"."movie_id") ON "movie_companies"."movie_id" = "t0"."id") ON "cast_info"."movie_id" = "t0"."movie_id"
GROUP BY "aka_name"."name", "cast_info"."role_id", "t0"."title", "t0"."production_year") AS "t2"
WHERE "t2"."TOTAL_MOVIES" > 1