SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "COMPANY_TYPE", "TOTAL_CAST", "AVG_MOVIE_RATING"
FROM (SELECT "ACTOR_NAME", "MOVIE_TITLE", "COMPANY_TYPE", "TOTAL_CAST", "AVG_MOVIE_RATING"
FROM (SELECT "aka_name"."name", "s1"."title", 'Production' AS "kind", "movie_info"."info", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("s1"."title") AS "MOVIE_TITLE", ANY_VALUE("t1"."kind") AS "COMPANY_TYPE", COUNT(*) AS "TOTAL_CAST", AVG(CAST("movie_info"."info" AS DECIMAL(19, 0))) AS "AVG_MOVIE_RATING"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Production') AS "t1"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."movie_info" INNER JOIN ("s1" INNER JOIN "IMDB"."movie_companies" ON "s1"."id" = "movie_companies"."movie_id") ON "movie_info"."movie_id" = "s1"."id") ON "cast_info"."movie_id" = "s1"."id") ON "t1"."id" = "movie_companies"."company_type_id"
GROUP BY "aka_name"."name", "s1"."title", "movie_info"."info") AS "t4"
WHERE "t4"."TOTAL_CAST" > 3
ORDER BY "AVG_MOVIE_RATING" DESC NULLS FIRST, "TOTAL_CAST" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t7"