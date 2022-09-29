SELECT
    iata_empresa_aerea,
    ac.razao_social,
    oad.name as nome_origem,
    icao_aerodromo_origem as icao_origem,
    oad.state as estado_origem,
    dad.name as nome_destino,
    icao_aerodromo_destino as icao_destino,
    dad.state as estado_destino,
    num_voos
FROM (
    -- Rota mais frequente para cada empresa aérea
    SELECT
       iata_empresa_aerea,
       icao_aerodromo_origem,
       icao_aerodromo_destino,
       COUNT(CONCAT(icao_aerodromo_origem, '_', icao_aerodromo_destino)) AS num_voos,
       -- Falta remover trajetos empatados em numeros
       row_number() OVER (PARTITION BY iata_empresa_aerea ORDER BY count(*) desc) AS rank
    FROM vra
    GROUP BY iata_empresa_aerea, icao_aerodromo_origem, icao_aerodromo_destino
    ORDER BY num_voos desc
) AS vra_most
LEFT JOIN air_cia ac ON vra_most.iata_empresa_aerea = ac.iata
LEFT JOIN aerodromo oad ON vra_most.icao_aerodromo_origem = oad.icao
LEFT JOIN aerodromo dad ON vra_most.icao_aerodromo_destino = dad.icao
WHERE rank = 1
ORDER BY vra_most.num_voos DESC