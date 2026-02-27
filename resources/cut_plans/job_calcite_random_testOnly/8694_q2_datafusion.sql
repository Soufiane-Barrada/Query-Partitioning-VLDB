SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."COMPANY_TYPE", "t3"."CAST_ORDER", "t3"."MOVIE_INFO", "t3"."MOVIE_KEYWORD", "t3"."production_year"
FROM (SELECT "s1"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "t1"."kind" AS "COMPANY_TYPE", "s1"."nr_order" AS "CAST_ORDER", "s1"."info" AS "MOVIE_INFO", "s1"."keyword" AS "MOVIE_KEYWORD", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Distributor') AS "t1"
INNER JOIN "s1" ON "t1"."id" = "s1"."company_type_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."name") AS "t3"