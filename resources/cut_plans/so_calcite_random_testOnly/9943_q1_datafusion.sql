SELECT COALESCE(ANY_VALUE("Users"."Id"), ANY_VALUE("Users"."Id")) AS "USERID", "Users"."DisplayName" AS "DISPLAYNAME", COUNT("Comments"."Id") AS "TOTALCOMMENTS", MAX("Comments"."CreationDate") AS "LASTCOMMENTDATE"
FROM "STACK"."Comments"
RIGHT JOIN "STACK"."Users" ON "Comments"."UserId" = "Users"."Id"
GROUP BY "Users"."Id", "Users"."DisplayName"