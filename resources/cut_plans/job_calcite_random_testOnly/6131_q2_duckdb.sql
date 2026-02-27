SELECT COALESCE("t5"."ACTOR_NAME", "t5"."ACTOR_NAME") AS "ACTOR_NAME", "t5"."MOVIE_TITLE", "t5"."PRODUCTION_YEAR", "t5"."COMPANY_TYPES", "t5"."KEYWORDS", "t5"."name"
FROM (SELECT ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t1"."title") AS "MOVIE_TITLE", "t1"."production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "s1"."kind", ',') AS "COMPANY_TYPES", LISTAGG(DISTINCT "keyword"."keyword", ',') AS "KEYWORDS", "aka_name"."name"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000 AND "production_year" <= 2020) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "s1"."id" = "movie_companies"."company_type_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t1"."id" = "movie_keyword"."movie_id") ON "cast_info"."movie_id" = "t1"."movie_id"
GROUP BY "aka_name"."name", "t1"."title", "t1"."production_year"
ORDER BY "t1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t5"