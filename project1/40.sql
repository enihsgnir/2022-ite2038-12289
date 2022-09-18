SELECT DISTINCT t.name,
                c.description
FROM   Trainer AS t
       JOIN Gym AS g
         ON t.id = g.leader_id
       JOIN CaughtPokemon AS cp
         ON t.id = cp.owner_id
       JOIN Pokemon AS p
         ON cp.pid = p.id
       JOIN City AS c
         ON t.hometown = c.name
WHERE  p.id IN (SELECT e.after_id
                FROM   Evolution AS e)
       AND ( p.type = 'Fire'
              OR p.type = 'Water'
              OR p.type = 'Grass' )
ORDER  BY t.name;
