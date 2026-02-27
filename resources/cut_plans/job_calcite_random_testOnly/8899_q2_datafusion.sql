SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."PRODUCTION_YEAR", "t3"."MOVIE_KEYWORD", "t3"."COMPANY_TYPE"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "s1"."production_year" AS "PRODUCTION_YEAR", "keyword"."keyword" AS "MOVIE_KEYWORD", "t1"."kind" AS "COMPANY_TYPE"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%Production%') AS "t1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN "s1" ON "movie_keyword"."movie_id" = "s1"."id") ON "cast_info"."movie_id" = "s1"."id") ON "t1"."id" = "s1"."company_type_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "aka_name"."name"
FETCH NEXT 100 ROWS ONLY) AS "t3"