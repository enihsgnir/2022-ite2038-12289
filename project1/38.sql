SELECT DISTINCT t.name,
                COUNT(t.name)
FROM   (SELECT qry.owner_id
        FROM   (SELECT cp.owner_id,
                       p.type
                FROM   CaughtPokemon AS cp
                       JOIN Pokemon AS p
                         ON cp.pid = p.id
                GROUP  BY cp.owner_id,
                          p.type) AS qry
        GROUP  BY qry.owner_id
        HAVING COUNT(qry.owner_id) = 2) AS qry2
       JOIN Trainer AS t
         ON qry2.owner_id = t.id
       JOIN CaughtPokemon AS cp2
         ON qry2.owner_id = cp2.owner_id
GROUP  BY t.name
ORDER  BY t.name;
