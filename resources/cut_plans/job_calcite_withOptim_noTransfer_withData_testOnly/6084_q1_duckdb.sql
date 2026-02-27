SELECT COALESCE("name", "name") AS "name", "title", "kind", "info", "ACTOR_NAME", "MOVIE_TITLE", "COMPANY_TYPE", "TOTAL_CAST", "AVG_MOVIE_RATING"
FROM (SELECT "aka_name"."name", "t0"."title", 'Production' AS "kind", "movie_info"."info", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", ANY_VALUE("t"."kind") AS "COMPANY_TYPE", COUNT(*) AS "TOTAL_CAST", AVG(CAST("movie_info"."info" AS DECIMAL(19, 0))) AS "AVG_MOVIE_RATING"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Production') AS "t"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."movie_info" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "movie_info"."movie_id" = "t0"."id") ON "cast_info"."movie_id" = "t0"."id") ON "t"."id" = "movie_companies"."company_type_id"
GROUP BY "aka_name"."name", "t0"."title", "movie_info"."info") AS "t3"
WHERE "t3"."TOTAL_CAST" > 3