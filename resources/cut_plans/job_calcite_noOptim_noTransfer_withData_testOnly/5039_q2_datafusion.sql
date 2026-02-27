SELECT COALESCE(ANY_VALUE("name"), ANY_VALUE("name")) AS "ACTOR_NAME", ANY_VALUE("title") AS "MOVIE_TITLE", "production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "keyword", ', ') AS "KEYWORDS", ANY_VALUE("kind") AS "CAST_TYPE", ANY_VALUE("name0") AS "COMPANY_NAME", "name"
FROM "s1"
WHERE "production_year" > 2000 AND ("kind" = 'Actor' OR "kind" = 'Producer')
GROUP BY "name", "title", "production_year", "kind", "name0"
ORDER BY "production_year" DESC NULLS FIRST, "name"