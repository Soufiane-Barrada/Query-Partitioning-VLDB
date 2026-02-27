SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "name", "t0"."title", "t0"."production_year" AS "PRODUCTION_YEAR", "t"."kind", "company_name"."name" AS "name0", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", ANY_VALUE("t"."kind") AS "CAST_TYPE", ANY_VALUE("company_name"."name") AS "COMPANY_NAME"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
INNER JOIN ((SELECT *
FROM "IMDB"."comp_cast_type"
WHERE "kind" IN ('Actor', 'Producer')) AS "t" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "t"."id" = "cast_info"."person_role_id" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t0"."id" = "movie_keyword"."movie_id") ON "cast_info"."movie_id" = "t0"."movie_id") ON "movie_companies"."movie_id" = "t0"."id"
GROUP BY "aka_name"."name", "t0"."title", "t0"."production_year", "t"."kind", "company_name"."name"