SELECT COALESCE(ANY_VALUE("aka_name"."name"), ANY_VALUE("aka_name"."name")) AS "ACTOR_NAME", ANY_VALUE("aka_title"."title") AS "MOVIE_TITLE", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE", COUNT(*) AS "TOTAL_CAST", AVG(CAST("movie_info"."info" AS DECIMAL(19, 0))) AS "AVG_MOVIE_RATING"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."aka_title" ON "cast_info"."movie_id" = "aka_title"."id"
INNER JOIN "IMDB"."movie_companies" ON "aka_title"."id" = "movie_companies"."movie_id"
INNER JOIN "IMDB"."company_type" ON "movie_companies"."company_type_id" = "company_type"."id"
INNER JOIN "IMDB"."movie_info" ON "aka_title"."id" = "movie_info"."movie_id"
WHERE "aka_title"."production_year" > 2000 AND "company_type"."kind" = 'Production'
GROUP BY "aka_name"."name", "aka_title"."title", "company_type"."kind", "movie_info"."info"
HAVING COUNT(*) > 3