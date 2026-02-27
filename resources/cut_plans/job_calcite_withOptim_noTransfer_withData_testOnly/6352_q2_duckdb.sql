SELECT COALESCE("t5"."ACTOR_NAME", "t5"."ACTOR_NAME") AS "ACTOR_NAME", "t5"."MOVIE_TITLE", "t5"."production_year" AS "PRODUCTION_YEAR", "t5"."COMPANY_TYPE", "t5"."MOVIE_KEYWORD", "t5"."ROLE_DESCRIPTION", "t5"."TOTAL_PEOPLE_INVOLVED"
FROM (SELECT "aka_name"."name" AS "name0", "t2"."title", "t2"."production_year", "t0"."kind", "t1"."keyword", "s1"."role", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t2"."title") AS "MOVIE_TITLE", ANY_VALUE("t0"."kind") AS "COMPANY_TYPE", ANY_VALUE("t1"."keyword") AS "MOVIE_KEYWORD", ANY_VALUE("s1"."role") AS "ROLE_DESCRIPTION", COUNT(DISTINCT "person_info"."id") AS "TOTAL_PEOPLE_INVOLVED"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" IN ('Distribution', 'Production')) AS "t0"
INNER JOIN ("IMDB"."company_name" INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id") ON "t0"."id" = "movie_companies"."company_type_id"
INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%') AS "t1" INNER JOIN "IMDB"."movie_keyword" ON "t1"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."id" = "person_info"."person_id" INNER JOIN ("s1" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t2" ON "complete_cast"."movie_id" = "t2"."id") ON "s1"."movie_id" = "t2"."id") ON "aka_name"."person_id" = "s1"."person_id") ON "movie_keyword"."movie_id" = "t2"."id") ON "movie_companies"."movie_id" = "t2"."id"
GROUP BY "t0"."kind", "t1"."keyword", "aka_name"."name", "s1"."role", "t2"."title", "t2"."production_year"
ORDER BY 12 DESC NULLS FIRST) AS "t5"