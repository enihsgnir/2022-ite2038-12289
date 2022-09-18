SELECT DISTINCT t.name
FROM   Trainer AS t
       JOIN CaughtPokemon AS cp
         ON t.id = cp.owner_id
       JOIN Pokemon AS p
         ON cp.pid = p.id
WHERE  p.type = 'Psychic'
ORDER  BY t.name;
