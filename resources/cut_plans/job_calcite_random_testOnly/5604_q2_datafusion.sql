SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."COMPANY_TYPE", "t3"."MOVIE_KEYWORD", "t3"."PERSON_INFO"
FROM (SELECT "s1"."name" AS "ACTOR_NAME", "t1"."title" AS "MOVIE_TITLE", "t0"."kind" AS "COMPANY_TYPE", "keyword"."keyword" AS "MOVIE_KEYWORD", "s1"."info" AS "PERSON_INFO"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Production') AS "t0"
INNER JOIN ("IMDB"."cast_info" INNER JOIN "s1" ON "cast_info"."person_id" = "s1"."person_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ((SELECT *
FROM "IMDB"."title"
WHERE "production_year" >= 2000) AS "t1" INNER JOIN "IMDB"."movie_companies" ON "t1"."id" = "movie_companies"."movie_id") ON "movie_keyword"."movie_id" = "t1"."id") ON "cast_info"."movie_id" = "t1"."id") ON "t0"."id" = "movie_companies"."company_type_id"
ORDER BY "s1"."name", "t1"."title") AS "t3"