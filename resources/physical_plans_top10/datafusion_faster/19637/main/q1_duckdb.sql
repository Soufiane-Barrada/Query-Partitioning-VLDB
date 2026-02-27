SELECT Id, Title, CreationDate, OwnerUserId, Tags, Score
FROM Posts
WHERE PostTypeId = 1
ORDER BY Score DESC
LIMIT 10;