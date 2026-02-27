SELECT COALESCE("t5"."ACTOR_NAME", "t5"."ACTOR_NAME") AS "ACTOR_NAME", "t5"."MOVIE_TITLE", "t5"."PRODUCTION_YEAR", "t5"."KEYWORDS", "t5"."COMPANY_TYPE", "t5"."PERSON_INFO", "t5"."name"
FROM (SELECT "aka_name"."name", "s1"."title", "s1"."production_year" AS "PRODUCTION_YEAR", 'Distributor' AS "kind", "person_info"."info", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("s1"."title") AS "MOVIE_TITLE", ARRAY_AGG(DISTINCT "keyword"."keyword") AS "KEYWORDS", ANY_VALUE("t1"."kind") AS "COMPANY_TYPE", ANY_VALUE("person_info"."info") AS "PERSON_INFO"
FROM "IMDB"."person_info"
RIGHT JOIN ("IMDB"."cast_info" INNER JOIN "IMDB"."aka_name" ON "cast_info"."person_id" = "aka_name"."person_id" INNER JOIN ("IMDB"."movie_keyword" INNER JOIN ("IMDB"."company_name" INNER JOIN ("s1" INNER JOIN (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Distributor') AS "t1" ON "s1"."company_type_id" = "t1"."id") ON "company_name"."id" = "s1"."company_id") ON "movie_keyword"."movie_id" = "s1"."id0" INNER JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id") ON "cast_info"."movie_id" = "s1"."movie_id0") ON "person_info"."person_id" = "aka_name"."person_id"
GROUP BY "person_info"."info", "aka_name"."name", "s1"."title", "s1"."production_year"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t5"