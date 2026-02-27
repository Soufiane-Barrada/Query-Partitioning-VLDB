SELECT COALESCE("t4"."MOVIE_TITLE", "t4"."MOVIE_TITLE") AS "MOVIE_TITLE", "t4"."ACTOR_NAME", "t4"."ACTOR_INFO", "t4"."COMPANY_NAME", "t4"."MOVIE_KEYWORD", "t4"."TITLE_INFO", "t4"."production_year"
FROM (SELECT "t2"."title" AS "MOVIE_TITLE", "aka_name"."name" AS "ACTOR_NAME", "person_info"."info" AS "ACTOR_INFO", "t1"."name" AS "COMPANY_NAME", "t0"."keyword" AS "MOVIE_KEYWORD", "s1"."info" AS "TITLE_INFO", "t2"."production_year"
FROM "s1"
INNER JOIN ("IMDB"."cast_info" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "cast_info"."person_id" = "aka_name"."person_id" INNER JOIN ((SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%') AS "t0" INNER JOIN "IMDB"."movie_keyword" ON "t0"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."company_id" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t2" ON "complete_cast"."movie_id" = "t2"."id") ON "movie_companies"."movie_id" = "t2"."id") ON "movie_keyword"."movie_id" = "t2"."id") ON "cast_info"."id" = "complete_cast"."subject_id") ON "s1"."movie_id" = "t2"."id"
ORDER BY "t2"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t4"