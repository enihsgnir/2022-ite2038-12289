SELECT DISTINCT p.name
FROM   Pokemon AS p
WHERE  p.type = 'Water'
       AND p.id NOT IN (SELECT e.before_id
                        FROM   Evolution AS e)
ORDER  BY p.name;
