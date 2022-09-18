SELECT DISTINCT p.name
FROM   Pokemon AS p
WHERE  p.id NOT IN (SELECT cp.pid
                    FROM   CaughtPokemon AS cp)
ORDER  BY p.name;
