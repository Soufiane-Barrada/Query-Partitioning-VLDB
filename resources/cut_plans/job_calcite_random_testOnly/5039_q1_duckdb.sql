SELECT COALESCE("id", "id") AS "id", "kind"
FROM "IMDB"."comp_cast_type"
WHERE "kind" IN ('Actor', 'Producer')