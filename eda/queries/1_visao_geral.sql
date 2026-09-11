SELECT
    COUNT(*) AS total_registros,
    COUNT(DISTINCT id_municipio) AS municipios,
    COUNT(DISTINCT sigla_uf) AS ufs,
    COUNT(DISTINCT ano) AS anos,
    MIN(taxa_alfabetizacao_media) AS menor_taxa,
    AVG(taxa_alfabetizacao_media) AS taxa_media,
    MAX(taxa_alfabetizacao_media) AS maior_taxa
FROM indicador_municipio;

SELECT
    CASE
        WHEN taxa_alfabetizacao_media < 0.50 THEN '0-49%'
        WHEN taxa_alfabetizacao_media < 0.60 THEN '50-59%'
        WHEN taxa_alfabetizacao_media < 0.70 THEN '60-69%'
        WHEN taxa_alfabetizacao_media < 0.80 THEN '70-79%'
        WHEN taxa_alfabetizacao_media < 0.90 THEN '80-89%'
        ELSE '90-100%'
    END AS faixa,
    COUNT(*) AS quantidade
FROM indicador_municipio
GROUP BY
    CASE
        WHEN taxa_alfabetizacao_media < 0.50 THEN '0-49%'
        WHEN taxa_alfabetizacao_media < 0.60 THEN '50-59%'
        WHEN taxa_alfabetizacao_media < 0.70 THEN '60-69%'
        WHEN taxa_alfabetizacao_media < 0.80 THEN '70-79%'
        WHEN taxa_alfabetizacao_media < 0.90 THEN '80-89%'
        ELSE '90-100%'
    END
ORDER BY MIN(taxa_alfabetizacao_media);

SELECT
    ano,
    nome_municipio,
    sigla_uf,
    regiao,
    rede_label,
    taxa_alfabetizacao_media
FROM indicador_municipio
ORDER BY taxa_alfabetizacao_media ASC
LIMIT 10;