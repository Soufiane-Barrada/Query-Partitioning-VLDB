SELECT COALESCE("s1"."production_year", "s1"."production_year") AS "PRODUCTIONYEAR", COUNT(*) AS "TOTALMOVIES", LISTAGG(DISTINCT "s1"."title", '; ') AS "MOVIETITLES", LISTAGG(DISTINCT "s1"."name", '; ') AS "ACTORS", LISTAGG(DISTINCT "company_name"."name", '; ') AS "PRODUCTIONCOMPANIES", LISTAGG(DISTINCT "s1"."keyword", '; ') AS "KEYWORDS"
FROM "IMDB"."company_name"
INNER JOIN "IMDB"."movie_companies" ON "company_name"."id" = "movie_companies"."company_id"
INNER JOIN "s1" ON "movie_companies"."movie_id" = "s1"."id1"
GROUP BY "s1"."production_year"
ORDER BY "s1"."production_year" DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY