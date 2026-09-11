SELECT
    COUNT(*) AS total_registros,
    COUNT(DISTINCT id_aluno) AS alunos_unicos,
    COUNT(DISTINCT sigla_uf) AS ufs,
    COUNT(DISTINCT rede_label) AS redes
FROM base_modelagem_aluno;

SELECT
    alfabetizado_oficial,
    COUNT(*) AS alunos,
    ROUND(
        100.0 * COUNT(*) /
        (SELECT COUNT(*)
         FROM base_modelagem_aluno
         WHERE alfabetizado_oficial IS NOT NULL),
        2
    ) AS percentual
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
GROUP BY alfabetizado_oficial
ORDER BY alfabetizado_oficial;