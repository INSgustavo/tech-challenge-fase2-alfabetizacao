SELECT
    rede_label,
    COUNT(*) AS registros,
    AVG(taxa_alfabetizacao_media) AS taxa_media,
    MIN(taxa_alfabetizacao_media) AS menor_taxa,
    MAX(taxa_alfabetizacao_media) AS maior_taxa
FROM resumo_uf
GROUP BY rede_label
ORDER BY taxa_media DESC;

SELECT
    regiao,
    rede_label,
    COUNT(*) AS registros,
    AVG(taxa_alfabetizacao_media) AS taxa_media
FROM resumo_uf
WHERE rede_label <> 'total'
GROUP BY regiao, rede_label
ORDER BY regiao, taxa_media DESC;

SELECT
    sigla_uf,
    nome_uf,
    regiao,
    rede_label,
    AVG(taxa_alfabetizacao_media) AS media
FROM resumo_uf
WHERE rede_label <> 'total'
GROUP BY
    sigla_uf,
    nome_uf,
    regiao,
    rede_label
ORDER BY sigla_uf, media DESC;