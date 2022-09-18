SELECT SUM(cp.level)
FROM   CaughtPokemon AS cp
       JOIN Pokemon AS p
         ON cp.pid = p.id
WHERE  p.type <> 'Fire'
       AND p.type <> 'Grass'
       AND p.type <> 'Water'
       AND p.type <> 'Electric';
