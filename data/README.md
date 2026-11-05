# Dados de entrada / Input data

Os arquivos locais e resultados são ignorados pelo Git. Este guia permite preparar as entradas sem depender de caminhos do computador usado no desenvolvimento.

Local source files and outputs are ignored by Git. Prepare these inputs before running Compose or the pipeline.

## Estrutura mínima / Required layout

```text
data/
  ibge/population/       # Exatamente uma planilha .xls / exactly one .xls workbook
  inmet/2025/            # CSVs originais do ano / original yearly station CSVs
  inmet/boundary_2025/   # Horas UTC da borda / UTC boundary hours
```

### População / Population

Obtenha a planilha de estimativas municipais no [IBGE](https://www.ibge.gov.br/estatisticas/sociais/populacao/9103-estimativas-de-populacao.html). A leitura espera a aba **Municípios**, com seis células por linha: UF, código da UF, código municipal, nome, população e nota. O título precisa conter uma data de referência interpretável. A versão usada no desenvolvimento é `POP2025_20260828.xls`, referência 01/07/2025. Não renomeie um `.xlsx` para `.xls`.

Download the municipal estimates workbook from IBGE. The reader expects an `.xls` file, worksheet **Municípios**, six cells per row, and a reference date in the heading. The development version is `POP2025_20260828.xls`, reference 2025-07-01. Renaming an `.xlsx` extension does not convert its format.

### INMET e borda / INMET and boundary hours

Obtenha e extraia os CSVs de estações automáticas na [página de dados históricos do INMET](https://portal.inmet.gov.br/dadoshistoricos). Preserve o formato original: Latin-1, separador `;`, oito linhas de metadados e o cabeçalho na nona linha. Os arquivos esperados pelo executor têm extensão `.CSV`.

Para completar 31/12/2025 no calendário local brasileiro, coloque em `boundary_2025` os arquivos das primeiras horas UTC de **01/01/2026**. No recorte usado, são preservadas as horas **0000 a 0500 UTC**, junto com as oito linhas de metadados e o cabeçalho original de cada estação. Essas linhas devem ser extraídas dos CSVs originais de 2026; não são medições inventadas ou geradas por uma API do projeto. Não coloque somente linhas de medições sem os metadados.

Download and extract automatic-station CSVs from INMET. Preserve Latin-1 encoding, semicolon separators, the first eight metadata lines and the ninth-line header; use the expected `.CSV` extension. To complete the final Brazilian local day of 2025, `boundary_2025` contains original **2026-01-01, 0000–0500 UTC** observations plus each station’s original metadata/header. Extract those hours from the original 2026 files; do not fabricate observations or remove metadata.

A Gold usa apenas a borda necessária e compatível com as estações selecionadas. A falta de entradas obrigatórias interrompe a execução antes do processamento longo.

Gold uses only necessary boundary hours with compatible station metadata. Missing mandatory input files stop the runner before lengthy processing.

## Entradas coletadas automaticamente / API-generated inputs

```text
ibge/municipalities/<UF>.json
open_meteo/geocoding/<UF>.json
open_meteo/weather/2025/<UF>.json
```

São geradas por `consume`, a partir de IBGE e Open-Meteo. Para reaproveitar arquivos existentes, configure `PIPELINE_START_AT=bronze` no `.env` do Compose ou use `--start-at bronze` localmente. Os JSONs devem corresponder ao ano e às capitais esperadas.

The `consume` stage creates these files from IBGE and Open-Meteo. To reuse existing files, set Compose `PIPELINE_START_AT=bronze` or use the local `--start-at bronze` argument. Inputs must match the expected year and capitals.

`bronze/`, `silver/`, `gold/`, `lineage/` e `pipeline_status.json` são saídas geradas. Não crie JSONs vazios para contornar uma entrada ausente.

`bronze/`, `silver/`, `gold/`, `lineage/` and `pipeline_status.json` are generated outputs. Do not create empty JSON files to bypass missing input errors.
