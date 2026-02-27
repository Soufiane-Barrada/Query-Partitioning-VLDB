SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."COMPANY_TYPE", "t3"."MOVIE_KEYWORD", "t3"."PERSON_INFO", "t3"."production_year"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "company_type"."kind" AS "COMPANY_TYPE", "t1"."keyword" AS "MOVIE_KEYWORD", "person_info"."info" AS "PERSON_INFO", "s1"."production_year"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."cast_info" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "cast_info"."person_id" = "aka_name"."person_id" INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%') AS "t1" INNER JOIN "IMDB"."movie_keyword" ON "t1"."id" = "movie_keyword"."keyword_id" INNER JOIN ("s1" INNER JOIN "IMDB"."movie_companies" ON "s1"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "s1"."id") ON "cast_info"."movie_id" = "s1"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
ORDER BY "aka_name"."name", "s1"."production_year" DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t3"