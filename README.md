# projetas_teste

## Dependências

### Criação de virtual env por conda

- `conda create -n "projetas_teste" python=3.7 pyspark ipykernel`

### Ou instalação dos requirements

- `pip3 install -r requirements.txt`

## Carregamento de dados

### VRA

- Nome das colunas foram transformados para snake_case.
- Coluna `icao_empresa_aerea` contém o código IATA das empresas aéreas, sendo assim, foi renomeado para `iata_empresa_aerea`
- Colunas com timestamp em string foram convertidas para timestamp compatível com pyspark
	- `partida_prevista`
	- `chegada_prevista`
	- `partida_real`
	- `chegada_real`

### AIR_CIA

- Nome das colunas foram transformados para snake_case, mantendo apenas o símbulo `-`
- Colunas `iata` e `icao` geradas ao separar `icao_iata`

### aerodromos

- Conexão com API realizada utilizando uma chave própria para a conta conectada ao serviço (Precisa incluir na célula 24 em "Coletar IATA e ICAO").
- Utiliza todos os ICAO coletados na tabela de voos para formar a tabela de aeródromos.
- A estrutura de dados dessa tabela é reforçada pelas funções de tipagem do pyspark para evitar conflitos de tipos ao gravar o dataframe.

## Views

### Rota mais utilizada para cada companhia aérea

contabiliza a ocorrência de rotas concatenando os campos de ICAO de origem e destino agrupados em conjunto com IATA da empresa aérea.

```sql
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
```

### Companhia aérea de maior atuação no ano para o aeroporto

Contabiliza através de 3 subtabelas diferentes:
- Total de voos no aeroporto por uma empresa aérea
- Total de voos da empresa aérea que partiram do aeroporto
- Total de voos da empresa aérea que tinham o aeroporto como destino

Sendo que a primeira tabela determina qual empresa aérea teve mais voos relacionados com o aeroporto em questão, agregando a contagem das outras duas tabelas na visualização.

Caso se trate de voos, partidas ou destinos realizados, basta acrescentar a condicional `WHERE situacao_voo = "REALIZADO"` nas subtabelas.

```sql
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
```