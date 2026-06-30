# Playbook do time

## Ritual mínimo

- **Sync de 15 minutos:** bloqueios, mudanças de contrato e dependências.
- **PR diário quando houver código utilizável:** reduz integração tardia.
- **Revisão cruzada:** ninguém aprova apenas o próprio domínio.
- **Teste integrado:** ao menos uma execução completa antes da entrega.

## Acordo de branches

```text
main       versão demonstrável
  └─ develop       integração do time
       ├─ feature/platform-observability
       ├─ feature/bronze-sources
       ├─ feature/streaming-quarantine
       └─ feature/gold-mlflow
```

## Modelo de PR

Cada PR deve responder:

1. O que mudou?
2. Qual requisito ou risco resolve?
3. Como foi testado?
4. Alterou schema, chave ou regra de negócio?
5. Há impacto em custo, segurança ou reprocessamento?
6. Qual evidência comprova o resultado?

## Regras de integração

- mudança no contrato exige atualização de `CONTRACT.md`;
- mudança de coluna exige dicionário atualizado;
- notebook não pode depender de variável criada manualmente em outro notebook;
- nenhuma task deve ocultar erro com `except Exception` sem registrar falha;
- dados de demonstração devem ser públicos, sintéticos ou anonimizados;
- resultado numérico no README precisa ser reproduzível.

## Responsabilidade coletiva

Mesmo com divisão por pessoa, o time inteiro responde por:

- coerência da arquitetura;
- capacidade de reexecução;
- qualidade da Gold;
- segurança dos segredos;
- clareza da apresentação;
- documentação da limitação do modelo.
