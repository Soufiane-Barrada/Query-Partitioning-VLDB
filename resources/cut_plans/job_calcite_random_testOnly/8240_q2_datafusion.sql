SELECT COALESCE("ACTOR_NAME", "ACTOR_NAME") AS "ACTOR_NAME", "MOVIE_TITLE", "ROLE_ID", "COMPANY_TYPE", "TOTAL_MOVIES", "name"
FROM (SELECT "ACTOR_NAME", "MOVIE_TITLE", "role_id" AS "ROLE_ID", "COMPANY_TYPE", "TOTAL_MOVIES", "name"
FROM (SELECT "aka_name"."name", "s1"."title", "cast_info"."role_id", "t1"."kind", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("s1"."title") AS "MOVIE_TITLE", ANY_VALUE("t1"."kind") AS "COMPANY_TYPE", COUNT(*) AS "TOTAL_MOVIES"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" IN ('Distributor', 'Production')) AS "t1"
INNER JOIN ("s1" INNER JOIN "IMDB"."movie_companies" ON "s1"."id" = "movie_companies"."movie_id") ON "t1"."id" = "movie_companies"."company_type_id"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "s1"."movie_id" = "cast_info"."movie_id"
GROUP BY "t1"."kind", "s1"."title", "aka_name"."name", "cast_info"."role_id") AS "t3"
WHERE "t3"."TOTAL_MOVIES" > 5
ORDER BY "TOTAL_MOVIES" DESC NULLS FIRST, "name"
FETCH NEXT 10 ROWS ONLY) AS "t6"