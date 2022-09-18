SELECT DISTINCT p.name
FROM   Pokemon AS p
       JOIN Evolution AS e
         ON p.id = e.before_id
WHERE  e.before_id > e.after_id
ORDER  BY p.name;
