SELECT COALESCE("t4"."ACTOR_NAME", "t4"."ACTOR_NAME") AS "ACTOR_NAME", "t4"."MOVIE_TITLE", "t4"."COMPANY_TYPE", "t4"."MOVIE_INFO", "t4"."MOVIE_KEYWORD", "t4"."production_year"
FROM (SELECT "t2"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "t1"."kind" AS "COMPANY_TYPE", "movie_info"."info" AS "MOVIE_INFO", "s1"."keyword" AS "MOVIE_KEYWORD", "s1"."production_year"
FROM "IMDB"."company_name"
INNER JOIN ((SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%Production%') AS "t1" INNER JOIN ("IMDB"."movie_info" INNER JOIN ((SELECT *
FROM "IMDB"."aka_name"
WHERE "name" LIKE '%Smith%') AS "t2" INNER JOIN "IMDB"."cast_info" ON "t2"."person_id" = "cast_info"."person_id" INNER JOIN "s1" ON "cast_info"."movie_id" = "s1"."id1") ON "movie_info"."movie_id" = "s1"."id1") ON "t1"."id" = "s1"."company_type_id") ON "company_name"."id" = "s1"."company_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "t2"."name") AS "t4"