SELECT AVG(qry.level)
FROM   (SELECT cp.level
        FROM   CaughtPokemon AS cp
               JOIN Pokemon AS p
                 ON cp.pid = p.id
        WHERE  p.type = 'Water') AS qry;
