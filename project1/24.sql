SELECT DISTINCT p.name
FROM   Pokemon AS p
       JOIN CaughtPokemon AS cp
         ON p.id = cp.pid
       JOIN Trainer AS t
         ON cp.owner_id = t.id
WHERE  t.hometown = 'Sangnok City'
        OR t.hometown = 'Brown City'
ORDER  BY p.name;
