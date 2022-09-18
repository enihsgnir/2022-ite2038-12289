SELECT COUNT(*)
FROM   CaughtPokemon AS cp
       JOIN Pokemon AS p
         ON cp.pid = p.id
WHERE  p.type IN ( 'Water', 'Electric', 'Psychic' );
