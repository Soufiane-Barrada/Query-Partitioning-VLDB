SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."COMPANY_KIND", "t3"."MOVIE_INFO", "t3"."production_year"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "t1"."title" AS "MOVIE_TITLE", "t0"."kind" AS "COMPANY_KIND", "s1"."info" AS "MOVIE_INFO", "t1"."production_year"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%Production%') AS "t0"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "s1"."movie_id" = "t1"."id") ON "cast_info"."movie_id" = "t1"."movie_id") ON "t0"."id" = "movie_companies"."company_type_id"
ORDER BY "t1"."production_year" DESC NULLS FIRST) AS "t3"