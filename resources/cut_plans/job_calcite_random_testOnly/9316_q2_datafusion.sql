SELECT COALESCE("t4"."AKA_NAME", "t4"."AKA_NAME") AS "AKA_NAME", "t4"."MOVIE_TITLE", "t4"."CAST_ORDER", "t4"."PERSON_INFO", "t4"."COMPANY_NAME", "t4"."MOVIE_KEYWORD", "t4"."ACTOR_ROLE", "t4"."production_year"
FROM (SELECT "s1"."name" AS "AKA_NAME", "s1"."title" AS "MOVIE_TITLE", "s1"."nr_order" AS "CAST_ORDER", "s1"."info" AS "PERSON_INFO", "company_name"."name" AS "COMPANY_NAME", "keyword"."keyword" AS "MOVIE_KEYWORD", "t2"."role" AS "ACTOR_ROLE", "s1"."production_year"
FROM (SELECT *
FROM "IMDB"."role_type"
WHERE "role" = 'Actor') AS "t2"
INNER JOIN ("s1" LEFT JOIN "IMDB"."company_name" ON "s1"."company_id" = "company_name"."id" LEFT JOIN "IMDB"."movie_keyword" ON "s1"."id1" = "movie_keyword"."movie_id" LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id") ON "t2"."id" = "s1"."role_id"
ORDER BY "s1"."production_year" DESC NULLS FIRST, "s1"."name") AS "t4"