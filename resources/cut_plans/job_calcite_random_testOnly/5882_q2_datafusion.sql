SELECT COALESCE("t4"."ACTOR_NAME", "t4"."ACTOR_NAME") AS "ACTOR_NAME", "t4"."MOVIE_TITLE", "t4"."PRODUCTION_YEAR", "t4"."COMPANY_TYPE", "t4"."MOVIE_KEYWORD"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "s1"."production_year" AS "PRODUCTION_YEAR", "t1"."kind" AS "COMPANY_TYPE", "t2"."keyword" AS "MOVIE_KEYWORD"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" ILIKE '%production%') AS "t1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" ILIKE '%action%') AS "t2" INNER JOIN "IMDB"."movie_keyword" ON "t2"."id" = "movie_keyword"."keyword_id" INNER JOIN "s1" ON "movie_keyword"."movie_id" = "s1"."id") ON "cast_info"."movie_id" = "s1"."id") ON "t1"."id" = "s1"."company_type_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t4"