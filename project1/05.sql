SELECT DISTINCT p.name
FROM   Pokemon AS p
       JOIN Evolution as e
         ON p.id = e.after_id
WHERE  e.before_id NOT IN (SELECT e2.after_id
                           FROM   Evolution AS e2)
ORDER  BY p.name;
