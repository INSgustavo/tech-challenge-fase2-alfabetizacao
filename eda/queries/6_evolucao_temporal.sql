SELECT
    ano,
    COUNT(*) AS registros,
    AVG(taxa_alfabetizacao_media) AS taxa_media,
    AVG(variacao_absoluta) AS variacao_media_absoluta,
    AVG(variacao_relativa) AS variacao_media_relativa
FROM evolucao_temporal
GROUP BY ano
ORDER BY ano;

SELECT
    ano,
    tendencia,
    COUNT(*) AS quantidade
FROM evolucao_temporal
GROUP BY ano, tendencia
ORDER BY ano, quantidade DESC;