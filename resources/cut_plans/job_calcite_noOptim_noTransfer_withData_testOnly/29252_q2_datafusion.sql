SELECT COALESCE("title", "title") AS "MOVIE_TITLE", "production_year" AS "PRODUCTION_YEAR", LISTAGG(DISTINCT "name", ', ') AS "ACTORS", LISTAGG(DISTINCT "kind", ', ') AS "COMPANIES", LISTAGG(DISTINCT "info", '; ') AS "INFORMATIONS", LISTAGG(DISTINCT "keyword", ', ') AS "KEYWORDS"
FROM "s1"
WHERE "production_year" >= 2000 AND "info_type_id" = (((SELECT "id" AS "ID"
FROM "IMDB"."info_type"
WHERE "info" = 'Synopsis')))
GROUP BY "title", "production_year"
ORDER BY "production_year" DESC NULLS FIRST, "title"