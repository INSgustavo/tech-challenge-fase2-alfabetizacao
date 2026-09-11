SELECT
    CASE
        WHEN nome_municipio IN (
            'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador',
            'Fortaleza', 'Brasília', 'Vitória', 'Goiânia', 'São Luís',
            'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém',
            'João Pessoa', 'Curitiba', 'Recife', 'Teresina',
            'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho',
            'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas'
        ) THEN 'Capital'
        ELSE 'Interior'
    END AS tipo_municipio,
    COUNT(*) AS registros,
    COUNT(DISTINCT id_municipio) AS municipios,
    AVG(taxa_alfabetizacao_media) AS taxa_media,
    MIN(taxa_alfabetizacao_media) AS menor_taxa,
    MAX(taxa_alfabetizacao_media) AS maior_taxa
FROM indicador_municipio
GROUP BY
    CASE
        WHEN nome_municipio IN (
            'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador',
            'Fortaleza', 'Brasília', 'Vitória', 'Goiânia', 'São Luís',
            'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém',
            'João Pessoa', 'Curitiba', 'Recife', 'Teresina',
            'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho',
            'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas'
        ) THEN 'Capital'
        ELSE 'Interior'
    END;

SELECT
    ano,
    CASE
        WHEN nome_municipio IN (
            'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador',
            'Fortaleza', 'Brasília', 'Vitória', 'Goiânia', 'São Luís',
            'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém',
            'João Pessoa', 'Curitiba', 'Recife', 'Teresina',
            'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho',
            'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas'
        ) THEN 'Capital'
        ELSE 'Interior'
    END AS tipo_municipio,
    COUNT(*) AS registros,
    AVG(taxa_alfabetizacao_media) AS taxa_media
FROM indicador_municipio
GROUP BY
    ano,
    CASE
        WHEN nome_municipio IN (
            'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador',
            'Fortaleza', 'Brasília', 'Vitória', 'Goiânia', 'São Luís',
            'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém',
            'João Pessoa', 'Curitiba', 'Recife', 'Teresina',
            'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho',
            'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas'
        ) THEN 'Capital'
        ELSE 'Interior'
    END
ORDER BY ano, tipo_municipio;

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