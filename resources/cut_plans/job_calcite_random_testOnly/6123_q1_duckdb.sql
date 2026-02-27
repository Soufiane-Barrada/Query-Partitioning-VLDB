SELECT COALESCE("movie_companies"."id", "movie_companies"."id") AS "id", "movie_companies"."movie_id", "movie_companies"."company_id", "movie_companies"."company_type_id", "movie_companies"."note", "t"."id" AS "id0", "t"."name", "t"."country_code", "t"."imdb_id", "t"."name_pcode_nf", "t"."name_pcode_sf", "t"."md5sum", "t0"."id" AS "id1", "t0"."kind"
FROM "IMDB"."movie_companies"
INNER JOIN (SELECT *
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA') AS "t" ON "movie_companies"."company_id" = "t"."id"
INNER JOIN (SELECT *
FROM "IMDB"."company_type"
WHERE "kind" = 'Distributor') AS "t0" ON "movie_companies"."company_type_id" = "t0"."id"