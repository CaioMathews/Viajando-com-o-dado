# Verificação local

Na raiz, com o ambiente virtual ativo:

```powershell
python -B -m unittest discover -s tests
node --test frontend/tests/*.test.mjs
```

Os testes cobrem coleta, recuperação de arquivos, proteção das fontes, orquestração, qualidade, cabeçalhos INMET, configuração da carga, contrato HTTP, busca aproximada, paginação e vínculos atuais. Spark/Java são necessários para os testes de arquivos e qualidade. O filesystem local do Windows usa o JAR do projeto.

Os testes de PostgreSQL são opcionais e exigem `TEST_POSTGRES_DSN` com um banco exclusivo para testes e permissão para criar schemas/bancos temporários. Não aponte os testes de carga para o banco da aplicação: a carga também publica o schema de vínculos atuais `pipeline_trace`. `TEST_GOLD_DIRECTORY` habilita o teste opcional com saídas Gold reais.

O acompanhamento de testes usa estado temporário ou `VCD_TRACKING_ENABLED=0`; não deve substituir `data/pipeline_status.json`. A suíte da auditoria antiga foi aposentada junto com o modelo de snapshots e substituída pelos contratos de dados atuais.
