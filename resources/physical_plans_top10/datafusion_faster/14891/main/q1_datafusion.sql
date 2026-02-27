SELECT p.Id AS PostId,
       COUNT(DISTINCT c.Id) AS CommentCount,
       COUNT(DISTINCT v.Id) AS VoteCount
FROM Posts p
LEFT JOIN Comments c ON p.Id = c.PostId
LEFT JOIN Votes v ON p.Id = v.PostId
WHERE p.CreationDate >= '2022-01-01'
GROUP BY p.Id;