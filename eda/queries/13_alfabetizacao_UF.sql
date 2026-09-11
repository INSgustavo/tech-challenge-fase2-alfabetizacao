SELECT
    sigla_uf,
    COUNT(*) AS alunos,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    AVG(alfabetizado_oficial) AS taxa_alfabetizacao
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
GROUP BY sigla_uf
ORDER BY taxa_alfabetizacao DESC;

SELECT
    sigla_uf,
    COUNT(*) AS alunos_avaliados,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    AVG(alfabetizado_oficial) AS taxa_alfabetizacao
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
  AND presenca_lp = 1
  AND preenchimento_lp = 1
GROUP BY sigla_uf
ORDER BY taxa_alfabetizacao DESC;