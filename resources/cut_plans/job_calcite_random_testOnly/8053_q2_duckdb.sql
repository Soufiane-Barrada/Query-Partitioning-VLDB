SELECT COALESCE("t4"."ACTOR_NAME", "t4"."ACTOR_NAME") AS "ACTOR_NAME", "t4"."MOVIE_TITLE", "t4"."COMPANY_TYPE", "t4"."MOVIE_KEYWORD", "t4"."ACTOR_INFO"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "t2"."title" AS "MOVIE_TITLE", "t1"."kind" AS "COMPANY_TYPE", "s1"."keyword" AS "MOVIE_KEYWORD", "person_info"."info" AS "ACTOR_INFO"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Distributor') AS "t1"
INNER JOIN ("IMDB"."cast_info" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "cast_info"."person_id" = "aka_name"."person_id" INNER JOIN ("s1" INNER JOIN "IMDB"."movie_keyword" ON "s1"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t2" INNER JOIN "IMDB"."movie_companies" ON "t2"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t2"."id") ON "cast_info"."movie_id" = "t2"."id") ON "t1"."id" = "movie_companies"."company_type_id"
ORDER BY "aka_name"."name", "t2"."title"
FETCH NEXT 100 ROWS ONLY) AS "t4"