SELECT DISTINCT t.name,
                t.hometown
FROM   Trainer AS t
       JOIN Gym AS g
         ON t.id = g.leader_id
       JOIN CaughtPokemon AS cp
         ON t.id = cp.owner_id
       JOIN Pokemon AS p
         ON cp.pid = p.id
WHERE  p.id NOT IN (SELECT e.before_id
                    FROM   Evolution AS e)
ORDER  BY t.name;
