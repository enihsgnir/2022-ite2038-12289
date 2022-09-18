SELECT SUM(cp.level)
FROM   CaughtPokemon AS cp
       JOIN Pokemon AS p
         ON cp.pid = p.id
WHERE  p.id NOT IN (SELECT e.before_id
                    FROM   Evolution AS e)
       AND p.id NOT IN (SELECT e2.after_id
                        FROM   Evolution AS e2);
