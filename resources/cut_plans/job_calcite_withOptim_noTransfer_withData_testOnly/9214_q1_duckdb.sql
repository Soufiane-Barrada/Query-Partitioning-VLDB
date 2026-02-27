SELECT COALESCE("t"."title", "t"."title") AS "title", "name"."name" AS "name0", "company_type"."kind", ANY_VALUE("t"."title") AS "MOVIE_TITLE", ANY_VALUE("name"."name") AS "ACTOR_NAME", COUNT(*) AS "ALIAS_COUNT", LISTAGG("keyword"."keyword", ',') AS "KEYWORDS", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE"
FROM "IMDB"."company_type"
INNER JOIN "IMDB"."movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."name" ON "aka_name"."person_id" = "name"."imdb_id" INNER JOIN ("IMDB"."cast_info" INNER JOIN ("IMDB"."complete_cast" INNER JOIN (SELECT *
FROM "IMDB"."title"
WHERE "production_year" > 2000) AS "t" ON "complete_cast"."movie_id" = "t"."id") ON "cast_info"."id" = "complete_cast"."subject_id") ON "aka_name"."person_id" = "cast_info"."person_id") ON "movie_keyword"."movie_id" = "t"."id") ON "movie_companies"."movie_id" = "t"."id"
GROUP BY "t"."title", "name"."name", "company_type"."kind"