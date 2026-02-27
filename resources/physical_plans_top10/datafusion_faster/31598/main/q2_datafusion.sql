SELECT COALESCE(t20.USERID, t20.USERID) AS USERID, t20.DISPLAYNAME, t20.TOTALSCORE, t20.TOTALPOSTS, t20.POSTID, t20.REVCOUNT, t20.USERCOUNT, t20.LASTREVISION, t20.TOTALDOWNVOTES
FROM (SELECT t18.USERID, t18.DISPLAYNAME, t18.TOTALSCORE, t18.TOTALPOSTS, s1.POSTID, s1.REVCOUNT, s1.USERCOUNT, s1.LASTREVISION, s1.TOTALDOWNVOTES
FROM (SELECT MIN(t14.Id) AS USERID, t14.DisplayName AS DISPLAYNAME, SUM(t14.Score) AS TOTALSCORE, COUNT(DISTINCT t14.Id0) AS TOTALPOSTS, COUNT(DISTINCT Badges.Id) AS TOTALBADGES, AVG(CASE WHEN t14.ViewCount IS NOT NULL THEN CAST(t14.ViewCount AS INTEGER) ELSE 0 END) AS AVGVIEWCOUNT
FROM (SELECT Users.Id, Users.Reputation, Users.CreationDate, Users.DisplayName, Users.LastAccessDate, Users.WebsiteUrl, Users.Location, Users.AboutMe, Users.Views, Users.UpVotes, Users.DownVotes, Users.ProfileImageUrl, Users.AccountId, Posts1.Id AS Id0, Posts1.PostTypeId, Posts1.AcceptedAnswerId, Posts1.ParentId, Posts1.CreationDate AS CreationDate0, Posts1.Score, Posts1.ViewCount, Posts1.Body, Posts1.OwnerUserId, Posts1.OwnerDisplayName, Posts1.LastEditorUserId, Posts1.LastEditorDisplayName, Posts1.LastEditDate, Posts1.LastActivityDate, Posts1.Title, Posts1.Tags, Posts1.AnswerCount, Posts1.CommentCount, Posts1.FavoriteCount, Posts1.ClosedDate, Posts1.CommunityOwnedDate, Posts1.ContentLicense
FROM Posts AS Posts1
RIGHT JOIN Users ON Posts1.OwnerUserId = Users.Id) AS t14
LEFT JOIN Badges ON t14.Id = Badges.UserId
GROUP BY t14.Id, t14.DisplayName
HAVING COUNT(DISTINCT t14.Id0) > 5 AND SUM(t14.Score) IS NOT NULL) AS t18
INNER JOIN s1 ON t18.USERID = s1.POSTID
ORDER BY t18.TOTALSCORE DESC NULLS FIRST, s1.USERCOUNT DESC NULLS FIRST) AS t20