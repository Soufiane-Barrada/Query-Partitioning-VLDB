SELECT COALESCE(ANY_VALUE("aka_name"."name"), ANY_VALUE("aka_name"."name")) AS "ACTOR_NAME", ANY_VALUE("title"."title") AS "MOVIE_TITLE", "title"."production_year" AS "PRODUCTION_YEAR", ANY_VALUE("role_type"."role") AS "ROLE_NAME", LISTAGG(DISTINCT "keyword"."keyword", ', ') AS "KEYWORDS", COUNT(DISTINCT "cast_info"."id") AS "TOTAL_COACTORS", "title"."production_year" AS "production_year_"
FROM "IMDB"."aka_name"
INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id"
INNER JOIN "IMDB"."title" ON "cast_info"."movie_id" = "title"."id"
INNER JOIN "IMDB"."role_type" ON "cast_info"."role_id" = "role_type"."id"
LEFT JOIN "IMDB"."movie_keyword" ON "title"."id" = "movie_keyword"."movie_id"
LEFT JOIN "IMDB"."keyword" ON "movie_keyword"."keyword_id" = "keyword"."id"
WHERE "title"."production_year" >= 2000 AND "title"."production_year" <= 2023 AND "role_type"."role" LIKE '%Lead%'
GROUP BY "aka_name"."name", "title"."title", "title"."production_year", "role_type"."role"