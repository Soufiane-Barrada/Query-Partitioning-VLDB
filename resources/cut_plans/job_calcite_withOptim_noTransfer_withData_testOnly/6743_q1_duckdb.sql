SELECT COALESCE("aka_name"."name", "aka_name"."name") AS "name", "t0"."title", "t"."kind", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", ANY_VALUE("t"."kind") AS "COMPANY_TYPE", COUNT(DISTINCT "movie_keyword"."keyword_id") AS "KEYWORD_COUNT", MAX(CASE WHEN "movie_info"."info_type_id" = 1 THEN "movie_info"."info" ELSE NULL END) AS "MOVIE_TAGLINE", MAX(CASE WHEN "movie_info"."info_type_id" = 2 THEN "movie_info"."info" ELSE NULL END) AS "MOVIE_SYNOPSIS"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" LIKE 'Production%') AS "t"
INNER JOIN ("IMDB"."movie_keyword" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."movie_info" INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" > 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "movie_info"."movie_id" = "t0"."id") ON "cast_info"."movie_id" = "t0"."movie_id") ON "movie_keyword"."movie_id" = "t0"."id") ON "t"."id" = "movie_companies"."company_type_id"
GROUP BY "aka_name"."name", "t0"."title", "t"."kind"