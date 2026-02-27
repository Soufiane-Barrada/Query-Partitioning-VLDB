SELECT COALESCE("t5"."ACTOR_NAME", "t5"."ACTOR_NAME") AS "ACTOR_NAME", "t5"."MOVIE_TITLE", "t5"."PRODUCTION_YEAR", "t5"."KEYWORDS", "t5"."CAST_TYPE", "t5"."COMPANY_NAME", "t5"."ACTOR_INFO", "t5"."name"
FROM (SELECT ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t1"."title") AS "MOVIE_TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", ANY_VALUE("s1"."kind") AS "CAST_TYPE", ANY_VALUE("t0"."name") AS "COMPANY_NAME", ANY_VALUE("person_info"."info") AS "ACTOR_INFO", "aka_name"."name"
FROM "s1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "s1"."person_id" = "aka_name"."person_id"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."company_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t1" ON "complete_cast"."movie_id" = "t1"."id") ON "movie_companies"."movie_id" = "t1"."id") ON "movie_keyword"."movie_id" = "t1"."id") ON "s1"."movie_id" = "t1"."id"
GROUP BY "aka_name"."name", "t1"."title", "t1"."production_year", "s1"."kind", "t0"."name", "person_info"."info"
ORDER BY "t1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t5"