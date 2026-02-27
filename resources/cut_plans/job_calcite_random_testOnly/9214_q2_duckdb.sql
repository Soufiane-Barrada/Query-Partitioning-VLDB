SELECT COALESCE("t3"."MOVIE_TITLE", "t3"."MOVIE_TITLE") AS "MOVIE_TITLE", "t3"."ACTOR_NAME", "t3"."ALIAS_COUNT", "t3"."KEYWORDS", "t3"."COMPANY_TYPE"
FROM (SELECT "s1"."title", "name"."name" AS "name0", "company_type"."kind", ANY_VALUE("s1"."title") AS "MOVIE_TITLE", ANY_VALUE("name"."name") AS "ACTOR_NAME", COUNT(*) AS "ALIAS_COUNT", LISTAGG("keyword"."keyword", ',') AS "KEYWORDS", ANY_VALUE("company_type"."kind") AS "COMPANY_TYPE"
FROM "IMDB"."company_type"
INNER JOIN "IMDB"."movie_companies" ON "company_type"."id" = "movie_companies"."company_type_id"
INNER JOIN ("IMDB"."keyword" INNER JOIN "IMDB"."movie_keyword" ON "keyword"."id" = "movie_keyword"."keyword_id" INNER JOIN ("IMDB"."aka_name" INNER JOIN "IMDB"."name" ON "aka_name"."person_id" = "name"."imdb_id" INNER JOIN "s1" ON "aka_name"."person_id" = "s1"."person_id") ON "movie_keyword"."movie_id" = "s1"."id00") ON "movie_companies"."movie_id" = "s1"."id00"
GROUP BY "s1"."title", "name"."name", "company_type"."kind"
ORDER BY 6 DESC NULLS FIRST, 4
FETCH NEXT 50 ROWS ONLY) AS "t3"