SELECT COALESCE("t0"."title", "t0"."title") AS "TITLE", "aka_name"."name" AS "ACTOR_NAME", "cast_info"."role_id" AS "ROLE_ID", "movie_info"."info" AS "MOVIE_INFO", "s1"."keyword" AS "KEYWORD"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2023) AS "t0" INNER JOIN "IMDB"."movie_info" ON "t0"."id" = "movie_info"."movie_id" INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id") ON "cast_info"."movie_id" = "t0"."id") ON "movie_companies"."movie_id" = "t0"."id"