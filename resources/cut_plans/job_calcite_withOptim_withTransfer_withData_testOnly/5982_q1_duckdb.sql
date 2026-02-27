SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "name", "t0"."title", "t"."kind", "keyword"."keyword", "person_info"."info", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", ANY_VALUE("t"."kind") AS "COMPANY_TYPE", ANY_VALUE("keyword"."keyword") AS "MOVIE_KEYWORD", ANY_VALUE("person_info"."info") AS "PERSON_INFO", COUNT(DISTINCT "cast_info"."person_id") AS "TOTAL_CASTS"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE '%production%') AS "t"
INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."company_type_id"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id" INNER JOIN ("IMDB"."cast_info" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "IMDB"."keyword" ON "t0"."id" = "keyword"."id") ON "cast_info"."movie_id" = "t0"."movie_id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "movie_companies"."movie_id" = "t0"."id"
GROUP BY "t"."kind", "aka_name"."name", "person_info"."info", "t0"."title", "keyword"."keyword"