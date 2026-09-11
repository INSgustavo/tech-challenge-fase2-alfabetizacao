SELECT
    regiao,
    AVG(taxa_alfabetizacao_media) AS taxa_media
FROM resumo_uf
WHERE rede_label <> 'total'
GROUP BY regiao
ORDER BY taxa_media DESC;

SELECT
    sigla_uf,
    nome_uf,
    AVG(taxa_alfabetizacao_media) AS taxa_media
FROM resumo_uf
WHERE rede_label <> 'total'
GROUP BY sigla_uf, nome_uf
ORDER BY taxa_media DESC;

WITH medias AS (
    SELECT
        sigla_uf,
        nome_uf,
        AVG(taxa_alfabetizacao_media) AS media
    FROM indicador_municipio
    GROUP BY sigla_uf, nome_uf
),
estatisticas AS (
    SELECT
        i.sigla_uf,
        i.nome_uf,
        AVG(i.taxa_alfabetizacao_media) AS media,
        MIN(i.taxa_alfabetizacao_media) AS menor,
        MAX(i.taxa_alfabetizacao_media) AS maior,
        sqrt(
            AVG(
                (i.taxa_alfabetizacao_media - m.media) *
                (i.taxa_alfabetizacao_media - m.media)
            )
        ) AS desvio_padrao
    FROM indicador_municipio i
    JOIN medias m
        ON i.sigla_uf = m.sigla_uf
    GROUP BY i.sigla_uf, i.nome_uf
)
SELECT
    sigla_uf,
    nome_uf,
    media,
    menor,
    maior,
    desvio_padrao,
    maior - menor AS amplitude
FROM estatisticas
ORDER BY amplitude DESC;