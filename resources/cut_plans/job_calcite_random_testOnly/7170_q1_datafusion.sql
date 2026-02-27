SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "name", "t0"."title", "t0"."production_year", "role_type"."role", "t"."kind", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", ANY_VALUE("role_type"."role") AS "CHARACTER_NAME", ANY_VALUE("t"."kind") AS "COMPANY_TYPE", COUNT(DISTINCT "keyword"."keyword") AS "KEYWORD_COUNT"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE 'Producer%') AS "t"
INNER JOIN ("IMDB"."role_type" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "role_type"."id" = "cast_info"."role_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000 AND "production_year" <= 2023) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "cast_info"."movie_id" = "t0"."id") ON "t"."id" = "movie_companies"."company_type_id"
GROUP BY "t"."kind", "role_type"."role", "aka_name"."name", "t0"."title", "t0"."production_year"