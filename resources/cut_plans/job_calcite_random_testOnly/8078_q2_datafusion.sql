SELECT COALESCE("t5"."ACTOR_NAME", "t5"."ACTOR_NAME") AS "ACTOR_NAME", "t5"."MOVIE_TITLE", "t5"."COMPANY_TYPE", "t5"."MOVIE_KEYWORD", "t5"."PERSON_INFO", "t5"."production_year"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "t3"."title" AS "MOVIE_TITLE", "t2"."kind" AS "COMPANY_TYPE", "keyword"."keyword" AS "MOVIE_KEYWORD", "person_info"."info" AS "PERSON_INFO", "t3"."production_year"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE 'Production%') AS "t2"
INNER JOIN ("s1" INNER JOIN ("IMDB"."cast_info" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "cast_info"."person_id" = "aka_name"."person_id") ON "s1"."ID" = "person_info"."info_type_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t3" INNER JOIN "IMDB"."movie_companies" ON "t3"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t3"."id") ON "cast_info"."movie_id" = "t3"."id") ON "t2"."id" = "movie_companies"."company_type_id"
ORDER BY "t3"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t5"