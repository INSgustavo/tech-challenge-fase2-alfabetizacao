SELECT
    tp_dependencia,
    rede,
    rede_label,
    COUNT(*) AS alunos,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    SUM(CASE WHEN alfabetizado_oficial = 0 THEN 1 ELSE 0 END) AS nao_alfabetizados,
    AVG(alfabetizado_oficial) AS taxa_alfabetizacao
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
GROUP BY
    tp_dependencia,
    rede,
    rede_label
ORDER BY taxa_alfabetizacao DESC;