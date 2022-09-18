SELECT p.type
FROM   Pokemon AS p
WHERE  p.id IN (SELECT e.before_id
                FROM   Evolution AS e)
        OR p.id IN (SELECT e2.after_id
                    FROM   Evolution AS e2)
GROUP  BY p.type
HAVING COUNT(p.type) >= 3
ORDER  BY p.type
