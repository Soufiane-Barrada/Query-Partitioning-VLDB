SELECT s1.Title,
       s1.CreationDate,
       u.DisplayName,
       t.TagName
FROM s1
JOIN Users u ON s1.OwnerUserId = u.Id
JOIN Tags t ON s1.Tags LIKE CONCAT('%', t.TagName, '%')
ORDER BY s1.Score DESC
LIMIT 10;