SELECT COALESCE("name", "name") AS "name", "title", "ROLE_ID", "kind", "ACTOR_NAME", "MOVIE_TITLE", "COMPANY_TYPE", "TOTAL_MOVIES"
FROM (SELECT "aka_name"."name", "t0"."title", "cast_info"."role_id", "t"."kind", ANY_VALUE("aka_name"."name") AS "ACTOR_NAME", ANY_VALUE("t0"."title") AS "MOVIE_TITLE", ANY_VALUE("t"."kind") AS "COMPANY_TYPE", COUNT(*) AS "TOTAL_MOVIES"
FROM (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" IN ('Distributor', 'Production')) AS "t"
INNER JOIN ((SELECT *
FROM "IMDB"."aka_title"
WHERE "production_year" >= 2000) AS "t0" INNER JOIN "IMDB"."movie_companies" ON "t0"."id" = "movie_companies"."movie_id") ON "t"."id" = "movie_companies"."company_type_id"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id") ON "t0"."movie_id" = "cast_info"."movie_id"
GROUP BY "t"."kind", "t0"."title", "aka_name"."name", "cast_info"."role_id") AS "t2"
WHERE "t2"."TOTAL_MOVIES" > 5