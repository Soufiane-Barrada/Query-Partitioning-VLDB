SELECT COALESCE(t9.POSTID, t9.POSTID) AS POSTID, t9.POSTTYPEID, t9.CREATIONDATE, t9.SCORE, t9.VIEWCOUNT, t9.COMMENTCOUNT, t9.VOTECOUNT, t9.USERID, t9.REPUTATION, t9.BADGECOUNT, t9.USERPOSTVIEWCOUNT
FROM (SELECT t7.POSTID, t7.POSTTYPEID, t7.CREATIONDATE, t7.SCORE, t7.VIEWCOUNT, t7.COMMENTCOUNT, t7.VOTECOUNT, t6.USERID, t6.REPUTATION, t6.BADGECOUNT, t6.USERPOSTVIEWCOUNT
FROM (SELECT MIN(t4.Id) AS USERID, t4.Reputation AS REPUTATION, COUNT(DISTINCT t4.Id0) AS BADGECOUNT, SUM(Posts0.ViewCount) AS USERPOSTVIEWCOUNT
FROM Posts AS Posts0
RIGHT JOIN (SELECT Users.Id, Users.Reputation, Users.CreationDate, Users.DisplayName, Users.LastAccessDate, Users.WebsiteUrl, Users.Location, Users.AboutMe, Users.Views, Users.UpVotes, Users.DownVotes, Users.ProfileImageUrl, Users.AccountId, Badges.Id AS Id0, Badges.UserId, Badges.Name, Badges.Date, Badges.Class, Badges.TagBased
FROM Badges
RIGHT JOIN Users ON Badges.UserId = Users.Id) AS t4 ON Posts0.OwnerUserId = t4.Id
GROUP BY t4.Id, t4.Reputation) AS t6
INNER JOIN (SELECT s1.POSTID, s1.POSTTYPEID, s1.CREATIONDATE, s1.SCORE, s1.VIEWCOUNT, s1.COMMENTCOUNT, s1.VOTECOUNT, Users0.Id, Users0.Reputation, Users0.CreationDate, Users0.DisplayName, Users0.LastAccessDate, Users0.WebsiteUrl, Users0.Location, Users0.AboutMe, Users0.Views, Users0.UpVotes, Users0.DownVotes, Users0.ProfileImageUrl, Users0.AccountId
FROM Users AS Users0
INNER JOIN s1 ON Users0.Id = s1.POSTTYPEID0) AS t7 ON t6.USERID = t7.Id
ORDER BY t7.VIEWCOUNT DESC NULLS FIRST, t7.SCORE DESC NULLS FIRST) AS t9