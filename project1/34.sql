SELECT DISTINCT t.name,
                c.description
FROM   Trainer AS t
       JOIN CaughtPokemon AS cp
         ON t.id = cp.owner_id
       JOIN Pokemon AS p
         ON cp.pid = p.id
       JOIN City AS c
         ON t.hometown = c.name
WHERE  p.type = 'Fire'
ORDER  BY t.name;
