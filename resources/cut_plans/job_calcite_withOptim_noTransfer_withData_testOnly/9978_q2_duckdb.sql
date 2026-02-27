SELECT COALESCE("t4"."ACTOR_NAME", "t4"."ACTOR_NAME") AS "ACTOR_NAME", "t4"."MOVIE_TITLE", "t4"."COMPANY_TYPE", "t4"."COMPANY_NOTE", "t4"."MOVIE_INFO", "t4"."production_year"
FROM (SELECT "aka_name"."name", "t1"."title", "company_type"."kind", "movie_companies"."note" AS "note00", "s1"."info0", "t1"."production_year", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t1"."title") AS "MOVIE_TITLE", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE", ANY_VALUE("movie_companies"."note") AS "COMPANY_NOTE", ANY_VALUE("s1"."info0") AS "MOVIE_INFO"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("s1" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "s1"."movie_id" = "t1"."id") ON "cast_info"."movie_id" = "t1"."id") ON "company_type"."id" = "movie_companies"."company_type_id"
GROUP BY "company_type"."kind", "aka_name"."name", "s1"."info0", "t1"."title", "t1"."production_year", "movie_companies"."note"
ORDER BY "t1"."production_year" DESC NULLS FIRST, 7) AS "t4"