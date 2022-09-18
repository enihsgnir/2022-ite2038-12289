SELECT DISTINCT t.name
FROM   Trainer AS t
       JOIN CaughtPokemon AS cp
         ON t.id = cp.owner_id
WHERE  cp.level = (SELECT MAX(cp2.level)
                   FROM   CaughtPokemon AS cp2)
        OR cp .level = (SELECT MIN(cp3.level)
                        FROM   CaughtPokemon AS cp3)
ORDER  BY t.name;
