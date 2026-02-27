SELECT COALESCE("t3"."ACTOR_NAME", "t3"."ACTOR_NAME") AS "ACTOR_NAME", "t3"."MOVIE_TITLE", "t3"."CAST_TYPE", "t3"."ACTOR_INFO", "t3"."MOVIE_KEYWORD", "t3"."production_year"
FROM (SELECT "s1"."name" AS "ACTOR_NAME", "s1"."title" AS "MOVIE_TITLE", "s1"."kind" AS "CAST_TYPE", "person_info"."info" AS "ACTOR_INFO", "t1"."keyword" AS "MOVIE_KEYWORD", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."keyword"
WHERE "keyword" LIKE '%action%') AS "t1"
INNER JOIN ("IMDB"."movie_keyword" INNER JOIN ("IMDB"."person_info" RIGHT JOIN "s1" ON "person_info"."person_id" = "s1"."person_id") ON "movie_keyword"."movie_id" = "s1"."id0") ON "t1"."id" = "movie_keyword"."keyword_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."name") AS "t3"