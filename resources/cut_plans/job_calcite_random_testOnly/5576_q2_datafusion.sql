SELECT COALESCE("t5"."ACTOR_NAME", "t5"."ACTOR_NAME") AS "ACTOR_NAME", "t5"."MOVIE_TITLE", "t5"."CAST_TYPE", "t5"."PERSON_INFO", "t5"."MOVIE_KEYWORD", "t5"."production_year"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "t3"."kind" AS "CAST_TYPE", "person_info"."info" AS "PERSON_INFO", "keyword"."keyword" AS "MOVIE_KEYWORD", "s1"."production_year"
FROM "IMDB"."keyword"
INNER JOIN ((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Biography') AS "t2" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."person_info" ON "aka_name"."person_id" = "person_info"."person_id") ON "t2"."ID" = "person_info"."info_type_id" INNER JOIN ((SELECT *
FROM "IMDB"."comp_cast_type"
WHERE "kind" LIKE 'Actor%') AS "t3" INNER JOIN "IMDB"."cast_info" ON "t3"."id" = "cast_info"."person_role_id" INNER JOIN ("s1" INNER JOIN "IMDB"."movie_keyword" ON "s1"."id" = "movie_keyword"."movie_id") ON "cast_info"."movie_id" = "s1"."id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "keyword"."id" = "movie_keyword"."keyword_id"
ORDER BY "aka_name"."name", "s1"."production_year" DESC NULLS FIRST) AS "t5"