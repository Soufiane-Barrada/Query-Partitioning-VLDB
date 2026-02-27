SELECT COALESCE("t3"."TITLE", "t3"."TITLE") AS "TITLE", "t3"."ACTOR_NAME", "t3"."CAST_TYPE", "t3"."production_year"
FROM (SELECT "t1"."title" AS "TITLE", "aka_name"."name" AS "ACTOR_NAME", "comp_cast_type"."kind" AS "CAST_TYPE", "t1"."production_year"
FROM "IMDB"."comp_cast_type"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "comp_cast_type"."id" = "cast_info"."person_role_id"
INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "s1"."id" = "movie_companies"."company_id") ON "cast_info"."movie_id" = "t1"."id"
ORDER BY "t1"."production_year" DESC NULLS FIRST) AS "t3"