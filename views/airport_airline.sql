SELECT
    ad.name as icao_name,
    ad_to_cia.icao as icao,
    ad_to_cia.iata_empresa_aerea as iata,
    ac.razao_social as iata_razao_social,
    o_count.num_origem as origem_total,
    d_count.num_destino as destino_total,
    ad_to_cia.num_cia as voos_total
FROM (
    -- Agrupamento e contagem de relação com empresa aérea e aeroporto
    SELECT
        ad.icao,
        v.iata_empresa_aerea,
        COUNT(v.iata_empresa_aerea) as num_cia,
        row_number() OVER (PARTITION BY ad.icao ORDER BY count(*) desc) AS rank
    FROM aerodromo ad
    LEFT JOIN vra v ON v.icao_aerodromo_origem = ad.icao OR v.icao_aerodromo_destino = ad.icao
    WHERE v.situacao_voo = "REALIZADO"
    GROUP BY ad.icao, v.iata_empresa_aerea
) AS ad_to_cia
LEFT JOIN (
    -- Contagem de total de voos como origem da empresa no aeroporto
    SELECT
        v.icao_aerodromo_origem as icao_origem,
        v.iata_empresa_aerea as iata_origem,
        COUNT(v.iata_empresa_aerea) as num_origem
    FROM vra v
    -- WHERE v.situacao_voo = "REALIZADO" -- Filtra rotas efetivamente realizadas, ao comentar, totaliza rotas registradas
    GROUP BY v.icao_aerodromo_origem, v.iata_empresa_aerea
) as o_count
ON o_count.icao_origem = ad_to_cia.icao and o_count.iata_origem = ad_to_cia.iata_empresa_aerea
LEFT JOIN (
    -- Contagem de total de voos como destino da empresa no aeroporto
    SELECT
        v.icao_aerodromo_destino as icao_destino,
        v.iata_empresa_aerea as iata_destino,
        COUNT(v.iata_empresa_aerea) as num_destino
    FROM vra v
    -- WHERE v.situacao_voo = "REALIZADO" -- Filtra rotas efetivamente realizadas, ao comentar, totaliza rotas registradas
    GROUP BY v.icao_aerodromo_destino, v.iata_empresa_aerea
) as d_count
ON d_count.icao_destino = ad_to_cia.icao and d_count.iata_destino = ad_to_cia.iata_empresa_aerea
LEFT JOIN aerodromo ad ON ad.icao = ad_to_cia.icao
LEFT JOIN air_cia ac ON ac.iata = ad_to_cia.iata_empresa_aerea

WHERE ad_to_cia.rank = 1
ORDER BY ad_to_cia.num_cia DESC