SELECT DISTINCT p.name,
                cp.level
FROM   CaughtPokemon AS cp
       JOIN Gym AS g
         ON cp.owner_id = g.leader_id
       JOIN Pokemon AS p
         ON cp.pid = p.id
WHERE  g.city = 'Sangnok City'
ORDER  BY cp.level,
          p.name;
