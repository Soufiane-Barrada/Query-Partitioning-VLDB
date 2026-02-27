SELECT COALESCE("t"."id", "t"."id") AS "id", "t"."name", "t"."country_code", "t"."imdb_id", "t"."name_pcode_nf", "t"."name_pcode_sf", "t"."md5sum", "movie_companies"."id" AS "id0", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note"
FROM (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t"
INNER JOIN "IMDB"."movie_companies" ON "t"."id" = "movie_companies"."company_id"