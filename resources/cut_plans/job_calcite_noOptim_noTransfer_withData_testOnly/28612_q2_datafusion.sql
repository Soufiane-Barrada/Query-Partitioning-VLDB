SELECT COALESCE("title", "title") AS "MOVIE_TITLE", "production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "name0", ', ') AS "ACTORS", LISTAGG(DISTINCT "keyword", ', ') AS "KEYWORDS", LISTAGG(DISTINCT "name" || ' (' || "kind" || ')', ', ') AS "COMPANIES"
FROM "s1"
WHERE "production_year" >= 2000 AND "production_year" <= 2023 AND "keyword" LIKE '%action%' AND "country_code" = 'USA'
GROUP BY "id", "title", "production_year"
ORDER BY "production_year" DESC NULLS FIRST, "title"