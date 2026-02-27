SELECT COALESCE("t2"."MOVIE_TITLE", "t2"."MOVIE_TITLE") AS "MOVIE_TITLE", "t2"."ACTOR_NAME", "t2"."ROLE_ID", "t2"."COMPANY_NAME", "t2"."MOVIE_KEYWORD", "t2"."production_year"
FROM (SELECT "t0"."title" AS "MOVIE_TITLE", "s1"."name" AS "ACTOR_NAME", "s1"."role_id" AS "ROLE_ID", "company_name"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", "t0"."production_year"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
INNER JOIN ("s1" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t0" ON "complete_cast"."movie_id" = "t0"."id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id") ON "t0"."id" = "movie_keyword"."movie_id") ON "s1"."person_id0" = "complete_cast"."subject_id") ON "movie_companies"."movie_id" = "t0"."id"
ORDER BY "t0"."production_year", "s1"."name") AS "t2"