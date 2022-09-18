SELECT DISTINCT t.name
FROM   Trainer AS t
       JOIN CaughtPokemon AS cp
         ON t.id = cp.owner_id
       JOIN Pokemon as p
         ON cp.pid = p.id
WHERE  p.id IN (SELECT e.after_id
                FROM   Evolution AS e)
       AND p.id NOT IN (SELECT e2.before_id
                        FROM   Evolution AS e2)
ORDER  BY t.name;
