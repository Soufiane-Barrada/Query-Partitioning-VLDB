SELECT COALESCE("id", "id") AS "id", "name", "country_code", "imdb_id", "name_pcode_nf", "name_pcode_sf", "md5sum"
FROM "IMDB"."company_name"
WHERE "country_code" = 'USA'