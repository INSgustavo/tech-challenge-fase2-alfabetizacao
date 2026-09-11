SELECT
    COUNT(*) AS alunos,
    COUNT(DISTINCT id_escola) AS escolas
FROM base_modelagem_aluno
WHERE id_escola IS NOT NULL;

SELECT
    id_escola,
    COUNT(*) AS alunos,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    AVG(alfabetizado_oficial) AS taxa_alfabetizacao
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
GROUP BY id_escola
HAVING COUNT(*) >= 10
ORDER BY taxa_alfabetizacao DESC
LIMIT 15;

SELECT
    id_escola,
    COUNT(*) AS alunos,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    AVG(alfabetizado_oficial) AS taxa_alfabetizacao
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
GROUP BY id_escola
HAVING COUNT(*) >= 10
ORDER BY taxa_alfabetizacao ASC
LIMIT 15;

SELECT
    id_escola,
    COUNT(*) AS alunos,
    SUM(CASE WHEN presenca_lp = 1 THEN 1 ELSE 0 END) AS presentes,
    SUM(CASE WHEN preenchimento_lp = 1 THEN 1 ELSE 0 END) AS preenchidos,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    AVG(presenca_lp) AS taxa_presenca,
    AVG(preenchimento_lp) AS taxa_preenchimento
FROM base_modelagem_aluno
WHERE id_escola IN (
    61412989,
    61416968,
    61421396,
    61421453,
    61427903,
    61427907,
    61428825,
    61433315,
    61436886,
    61437348,
    61437400
)
GROUP BY id_escola
ORDER BY id_escola;

SELECT
    id_escola,
    COUNT(*) AS alunos_avaliados,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    AVG(alfabetizado_oficial) AS taxa_alfabetizacao
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
  AND presenca_lp = 1
  AND preenchimento_lp = 1
GROUP BY id_escola
HAVING COUNT(*) >= 10
ORDER BY taxa_alfabetizacao DESC
LIMIT 15;

SELECT
    id_escola,
    COUNT(*) AS alunos_avaliados,
    SUM(CASE WHEN alfabetizado_oficial = 1 THEN 1 ELSE 0 END) AS alfabetizados,
    AVG(alfabetizado_oficial) AS taxa_alfabetizacao
FROM base_modelagem_aluno
WHERE alfabetizado_oficial IS NOT NULL
  AND presenca_lp = 1
  AND preenchimento_lp = 1
GROUP BY id_escola
HAVING COUNT(*) >= 10
ORDER BY taxa_alfabetizacao ASC
LIMIT 15;