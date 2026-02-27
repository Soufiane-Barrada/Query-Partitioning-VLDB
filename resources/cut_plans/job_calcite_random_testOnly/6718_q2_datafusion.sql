SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."ROLE_ORDER", "t3"."CAST_TYPE", "t3"."COMPANY_NAME", "t3"."MOVIE_INFO", "t3"."MOVIE_KEYWORD", "t3"."production_year"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "cast_info"."nr_order" AS "ROLE_ORDER", "t1"."kind" AS "CAST_TYPE", "company_name"."name" AS "COMPANY_NAME", "movie_info"."info" AS "MOVIE_INFO", "s1"."keyword" AS "MOVIE_KEYWORD", "s1"."production_year"
FROM "IMDB"."company_name"
INNER JOIN ((SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Distributor') AS "t1" INNER JOIN ("IMDB"."info_type" INNER JOIN "IMDB"."movie_info" ON "info_type"."id" = "movie_info"."info_type_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN "s1" ON "cast_info"."movie_id" = "s1"."id00") ON "movie_info"."movie_id" = "s1"."id00") ON "t1"."id" = "s1"."company_type_id") ON "company_name"."id" = "s1"."company_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t3"