SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."COMPANY_TYPE", "t3"."KEYWORD", "t3"."ADDITIONAL_INFO"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "t1"."title" AS "MOVIE_TITLE", "company_type"."kind" AS "COMPANY_TYPE", "keyword"."keyword" AS "KEYWORD", "movie_info"."info" AS "ADDITIONAL_INFO"
FROM "IMDB"."movie_info"
RIGHT JOIN ("IMDB"."company_name" INNER JOIN ("IMDB"."movie_companies" INNER JOIN ("IMDB"."keyword" INNER JOIN ("IMDB"."movie_keyword" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN ("IMDB"."aka_name" INNER JOIN "s1" ON "aka_name"."person_id" = "s1"."person_id") ON "t1"."movie_id" = "s1"."movie_id") ON "movie_keyword"."movie_id" = "t1"."id") ON "keyword"."id" = "movie_keyword"."keyword_id") ON "movie_companies"."movie_id" = "t1"."id" INNER JOIN "IMDB"."company_type" ON "movie_companies"."company_type_id" = "company_type"."id") ON "company_name"."id" = "movie_companies"."company_id") ON "movie_info"."movie_id" = "t1"."id"
ORDER BY "aka_name"."name", "t1"."title", "company_type"."kind"
FETCH NEXT 100 ROWS ONLY) AS "t3"