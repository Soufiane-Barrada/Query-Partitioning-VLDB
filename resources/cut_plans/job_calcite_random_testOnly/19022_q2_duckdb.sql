SELECT COALESCE("t2"."TITLE", "t2"."TITLE") AS "TITLE", "t2"."ACTOR_NAME", "t2"."COMPANY_TYPE"
FROM (SELECT "t0"."title" AS "TITLE", "s1"."name" AS "ACTOR_NAME", "company_type"."kind" AS "COMPANY_TYPE"
FROM "IMDB"."company_type"
INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" = 2020) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "company_type"."id" = "movie_companies"."company_type_id"
INNER JOIN "s1" ON "t0"."id" = "s1"."movie_id"
ORDER BY "t0"."title", "s1"."name") AS "t2"