SELECT COALESCE("t2"."id", "t2"."id") AS "id", "t2"."person_id", "t2"."info_type_id", "t2"."info", "t2"."note", "t2"."$f0" AS "FD_COL_5", "aka_name"."id" AS "id0", "aka_name"."person_id" AS "person_id0", "aka_name"."name", "aka_name"."imdb_index", "aka_name"."name_pcode_cf", "aka_name"."name_pcode_nf", "aka_name"."surname_pcode", "aka_name"."md5sum"
FROM (SELECT "person_info"."id", "person_info"."person_id", "person_info"."info_type_id", "person_info"."info", "person_info"."note", "t0"."$f0"
FROM (SELECT SINGLE_VALUE("id") AS "$f0"
FROM "IMDB"."info_type"
WHERE "info" = 'Biography') AS "t0",
"IMDB"."person_info"
WHERE "person_info"."info_type_id" = "t0"."$f0") AS "t2"
INNER JOIN "IMDB"."aka_name" ON "t2"."person_id" = "aka_name"."person_id"