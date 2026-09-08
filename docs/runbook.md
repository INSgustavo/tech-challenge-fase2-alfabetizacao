# Runbook de execução, falha e demonstração

## Execução padrão

1. Execute `00_setup_ambiente` (já prepara e copia as fontes oficiais internamente).
2. Suba manualmente o `TS_ALUNO.csv` no Volume, se ainda não estiver lá.
3. Execute Bronze batch e streaming.
4. Consulte as contagens Bronze.
5. Execute Silver.
6. Execute Quality Gate.
7. Somente com a qualidade aprovada, execute Gold.
8. Execute serving e MLflow.
9. Execute monitoramento e valide o `run_id`.
10. Execute `09_dashboard` e confira o Command Center renderizado.

## Falha na ingestão batch

- confirme nome e caminho do arquivo;
- valide delimitador, encoding e cabeçalho;
- compare schema observado com o contrato;
- não altere a Silver para acomodar silenciosamente um arquivo inesperado;
- registre a falha na auditoria e corrija a origem ou versione o contrato.

## Falha no streaming

- verifique a pasta de checkpoint;
- confirme que o checkpoint não está dentro da landing;
- valide `schema_version`;
- procure o evento na quarentena;
- para replay, corrija o payload e publique com novo registro de auditoria, preservando o `event_id` original em um campo de referência.

## Falha de qualidade

- diferencie falha por registro e falha sistêmica;
- registros inválidos devem ir para quarentena;
- volume zero, perda severa de cobertura ou duplicidade estrutural devem interromper a publicação;
- não sobrescreva a Gold anterior com uma carga reprovada.

## Demonstração recomendada

1. Mostre o diagrama e explique o grão.
2. Rode uma carga válida.
3. Envie um evento duplicado e mostre a deduplicação.
4. Envie um evento com UF inválida e mostre a quarentena.
5. Mostre a Silver canônica.
6. Execute os testes e publique a Gold.
7. Consulte um município.
8. Mostre o documento no MongoDB.
9. Mostre a execução no MLflow.
10. Abra o Command Center (`09_dashboard`) e navegue pelas abas.
11. Finalize com a tabela de auditoria.

## Evidências a capturar

- execução do Workflow;
- contagem por camada;
- registro em quarentena;
- resultado dos testes;
- consulta Gold;
- documento MongoDB;
- experimento MLflow;
- Command Center renderizado (print de pelo menos duas abas);
- métricas de custo e duração.
