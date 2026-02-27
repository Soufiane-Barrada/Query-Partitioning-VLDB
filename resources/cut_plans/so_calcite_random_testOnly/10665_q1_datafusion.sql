SELECT COALESCE("Id", "Id") AS "Id", "Reputation", "CreationDate", "DisplayName", "LastAccessDate", "WebsiteUrl", "Location", "AboutMe", "Views", "UpVotes", "DownVotes", "ProfileImageUrl", "AccountId"
FROM "STACK"."Users"
WHERE "Reputation" > 0