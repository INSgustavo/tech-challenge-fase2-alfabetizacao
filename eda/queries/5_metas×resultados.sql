SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN atingiu_meta = 1 THEN 1 ELSE 0 END) AS atingiram_meta,
    SUM(CASE WHEN atingiu_meta = 0 THEN 1 ELSE 0 END) AS nao_atingiram_meta,
    ROUND(
        100.0 * SUM(CASE WHEN atingiu_meta = 1 THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS percentual_atingimento
FROM meta_vs_resultado;

SELECT
    ano,
    sigla_uf,
    nome_municipio,
    rede_label,
    taxa_alfabetizacao_media,
    meta_taxa,
    gap_meta,
    atingiu_meta
FROM meta_vs_resultado
WHERE gap_meta IS NOT NULL
ORDER BY gap_meta ASC
LIMIT 10;

SELECT
    ano,
    sigla_uf,
    nome_municipio,
    rede_label,
    taxa_alfabetizacao_media,
    meta_taxa,
    gap_meta,
    atingiu_meta
FROM meta_vs_resultado
WHERE gap_meta IS NOT NULL
ORDER BY gap_meta DESC
LIMIT 10;