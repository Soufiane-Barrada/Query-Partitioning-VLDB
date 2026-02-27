SELECT COALESCE("t5"."MOVIE_TITLE", "t5"."MOVIE_TITLE") AS "MOVIE_TITLE", "t5"."PRODUCTION_YEAR", "t5"."ACTORS", "t5"."COMPANY_TYPES", "t5"."KEYWORDS"
FROM (SELECT ANY_VALUE("s1"."title") AS "MOVIE_TITLE", "s1"."production_year" AS "PRODUCTION_YEAR", LISTAGG("aka_name"."name", ', ') AS "ACTORS", LISTAGG("company_type"."kind", ', ') AS "COMPANY_TYPES", LISTAGG("keyword"."keyword", ', ') AS "KEYWORDS"
FROM "IMDB"."company_type"
INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."cast_info" ON "aka_name"."person_id" = "cast_info"."person_id" INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN "s1" ON "movie_keyword"."movie_id" = "s1"."id") ON "cast_info"."movie_id" = "s1"."id") ON "company_type"."id" = "s1"."company_type_id"
WHERE "s1"."production_year" >= 2000
GROUP BY "s1"."id", "s1"."title", "s1"."production_year"
HAVING LOWER(LISTAGG("aka_name"."name", ', ')) LIKE '%john%'
ORDER BY "s1"."production_year" DESC NULLS FIRST) AS "t5"