SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."CAST_TYPE", "t3"."ACTOR_INFO", "t3"."MOVIE_KEYWORD", "t3"."production_year"
FROM (SELECT "aka_name"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "comp_cast_type"."kind" AS "CAST_TYPE", "person_info"."info" AS "ACTOR_INFO", "t1"."keyword" AS "MOVIE_KEYWORD", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%') AS "t1"
INNER JOIN ("IMDB"."movie_keyword" INNER JOIN ("IMDB"."person_info" RIGHT JOIN ("IMDB"."aka_name" INNER JOIN ("s1" INNER JOIN ("IMDB"."cast_info" INNER JOIN "IMDB"."comp_cast_type" ON "cast_info"."person_role_id" = "comp_cast_type"."id") ON "s1"."id" = "cast_info"."movie_id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "person_info"."person_id" = "aka_name"."person_id") ON "movie_keyword"."movie_id" = "s1"."id") ON "t1"."id" = "movie_keyword"."keyword_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "aka_name"."name") AS "t3"