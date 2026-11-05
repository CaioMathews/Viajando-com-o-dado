"""Catálogo manual do fluxo. Descreve as operações; não executa transformações."""

from copy import deepcopy
from pathlib import Path


PROJECT_DIRECTORY = Path(__file__).resolve().parents[2]

SOURCES = [
    ("municipalities", "ibge_municipalities", "Municípios", "IBGE · API", "ibge/municipalities", "ingest_municipalities", "transform_municipalities", "build_municipalities"),
    ("population", "ibge_population", "População", "IBGE · Planilha", "ibge/population", "ingest_population", "transform_population", "build_population"),
    ("geocoding", "geocoding", "Coordenadas", "Open-Meteo · Geocoding", "open_meteo/geocoding", "ingest_geocoding", "transform_geocoding", "build_geocoding"),
    ("open_meteo_daily", "open_meteo", "Clima diário", "Open-Meteo · API", "open_meteo/weather/{year}", "ingest_open_meteo", "transform_open_meteo", "build_open_meteo"),
    ("inmet_daily", "inmet", "Estações INMET", "INMET · CSV", "inmet/{year}", "ingest_inmet", "transform_inmet", "build_inmet"),
    ("inmet_boundary", "inmet", "Borda INMET", "INMET · Borda UTC", "inmet/boundary_{year}", "ingest_inmet", "transform_inmet", None),
]

DESCRIPTIONS = {
    "bronze": {
        "municipalities": "Lê os JSONs das capitais, preserva a estrutura do IBGE e identifica a UF e o arquivo de origem.",
        "population": "Lê a planilha sem interpretar o cabeçalho: conserva aba, número da linha e células originais em JSON.",
        "geocoding": "Lê os JSONs de coordenadas, preserva os valores recebidos e identifica a origem e a UF.",
        "open_meteo_daily": "Preserva a resposta do clima, incluindo unidades, coordenadas e listas de observações diárias.",
        "inmet_daily": "Decodifica os CSVs em Latin-1, confere o cabeçalho e conserva metadados da estação, valores textuais e linha original.",
        "inmet_boundary": "Lê os arquivos de borda que permitem completar o calendário local nas extremidades do ano.",
    },
    "silver": {
        "municipalities": "Extrai as hierarquias do IBGE, padroniza nomes e códigos e reúne origens equivalentes.",
        "population": "Extrai o ID IBGE e os habitantes das células, obtém a referência do título e preserva notas e origens.",
        "geocoding": "Padroniza nomes, códigos e coordenadas; reúne duplicatas equivalentes e rejeita conflitos.",
        "open_meteo_daily": "Expande as listas em uma linha por município e dia. Padroniza tipos, converte vento de km/h para m/s e identifica ausências e valores inválidos.",
        "inmet_daily": "Interpreta números e horários UTC, converte marcadores -9999 em nulos, identifica inconsistências e consolida estação e horário com todas as origens.",
        "inmet_boundary": "Aplica o mesmo contrato das observações principais às horas de borda, preservando sua identificação separada.",
    },
    "gold": {
        "municipalities": "Seleciona as 27 capitais pelos IDs oficiais e publica o cadastro territorial de referência.",
        "population": "Relaciona população e cadastro oficial das capitais, preservando a data e as notas da estimativa.",
        "geocoding": "Relaciona coordenadas e cadastro oficial. Os nomes e as UFs vêm do IBGE.",
        "open_meteo_daily": "Relaciona clima e localização das capitais, completa o calendário e identifica a disponibilidade de cada variável.",
        "inmet_daily": "Relaciona capitais e estações, seleciona uma estação fixa elegível e agrega as observações no dia local. Usa a borda compatível quando necessária.",
    },
}

CHECKS = {
    "municipalities": ["IDs IBGE válidos e únicos.", "Coerência entre município, UF e região.", "Capitais reconhecidas pelos 27 IDs oficiais.", "Duplicatas equivalentes são consolidadas; conflitos interrompem a carga."],
    "population": ["Estrutura esperada das células e referência encontrada no título.", "População inteira e positiva.", "Nome e UF coerentes com o cadastro oficial.", "Estimativas de referências diferentes não são somadas."],
    "geocoding": ["Latitude entre -90 e 90; longitude entre -180 e 180.", "Nomes, IDs e UFs coerentes com o IBGE.", "Coordenadas conflitantes não são escolhidas arbitrariamente."],
    "open_meteo_daily": ["Unidades reconhecidas antes da conversão.", "Datas válidas, únicas e dentro do período declarado.", "Ausências, valores inválidos e alertas permanecem separados.", "Na Gold, somente a variável inválida é anulada."],
    "inmet_daily": ["Cabeçalho e metadados da estação conferidos.", "Número não interpretável e marcador de ausência são tratados separadamente.", "Regras físicas verificam chuva, vento, umidade, pressão e coerência das temperaturas.", "A estação precisa estar na mesma UF, a até 50 km e ter ao menos 90% de cobertura de temperatura e chuva no período.", "A média diária exige 90% das horas; somas e extremos exigem 100%.", "Chuva e extremos de intervalo usam a hora encerrada; temperatura e vento instantâneos usam o instante da mensagem.", "A borda exige a mesma estação e UF, com diferença cadastral de até 100 m."],
}


def catalog(year="2025"):
    nodes = []

    for row, definition in enumerate(SOURCES):
        dataset, source, title, origin, relative, ingest, transform, build = definition
        relative = relative.format(year=year)
        layers = ["source", "bronze", "silver"]

        if build:
            layers.extend(["gold", "load"])

        for layer in layers:
            node_id = layer + ":" + dataset
            function = ingest if layer == "bronze" else transform if layer == "silver" else build
            module = "ibge_municipalities" if dataset == "municipalities" else "ibge_population" if dataset == "population" else source
            file = f"pipeline/{layer}/{module}.py"
            description = DESCRIPTIONS.get(layer, {}).get(dataset)
            parents = []
            column = {"source": 0, "bronze": 1, "silver": 2, "gold": 3, "load": 4}[layer]
            path = relative if layer == "source" else layer + "/" + relative

            if layer == "source":
                file = "pipeline/consume APIs/consume.py" if dataset in {"municipalities", "geocoding", "open_meteo_daily"} else ""
                function = "consume" if file else ""
                description = f"Arquivos atuais de {origin}. A prévia usa a leitura bruta da Bronze; o arquivo original continua disponível na pasta da fonte."
            elif layer == "bronze":
                parents = ["source:" + dataset]
            elif layer == "silver":
                parents = ["bronze:" + dataset]
            elif layer == "gold":
                parents = ["silver:" + dataset]

                if dataset in {"population", "geocoding"}:
                    parents.append("gold:municipalities")

                if dataset in {"open_meteo_daily", "inmet_daily"}:
                    parents.append("gold:geocoding")

                if dataset == "inmet_daily":
                    parents.append("silver:inmet_boundary")
            else:
                parents = ["gold:" + dataset]
                file = "pipeline/load/postgres.py"
                function = "load_gold"
                description = "Publica a Gold no PostgreSQL por chave, na mesma transação das cinco tabelas e dos vínculos de rastreamento."

            key_fields = ["municipality_id"]

            if dataset == "population":
                key_fields = ["source_file", "source_sheet", "source_row_number"] if layer in {"source", "bronze"} else ["municipality_id", "reference_date"]
            elif dataset in {"inmet_daily", "inmet_boundary"}:
                key_fields = ["source_file", "source_row_number"] if layer in {"source", "bronze"} else ["station_code", "timestamp_utc"] if layer == "silver" else ["municipality_id", "observation_date"]
            elif dataset == "municipalities" and layer in {"source", "bronze"}:
                key_fields = ["id"]
            elif dataset == "open_meteo_daily" and layer not in {"source", "bronze"}:
                key_fields = ["municipality_id", "observation_date"]

            methods = []

            if function:
                methods.append({"name": function, "file": file, "description": description})

            if layer in {"bronze", "silver", "gold"}:
                methods.append({"name": "write_parquet", "file": "pipeline/shared/storage.py", "description": "Grava temporariamente, confere schema e quantidade e publica a saída com recuperação em caso de interrupção."})

            if layer == "gold" and dataset in {"open_meteo_daily", "inmet_daily"}:
                methods.append({"name": "validate_quality", "file": "pipeline/shared/quality.py", "description": "Confere a coerência entre medições, campos ausentes, campos inválidos e alertas."})

            if layer == "gold" and dataset == "inmet_daily":
                methods.append({"name": "write_inmet_links", "file": "pipeline/shared/lineage.py", "description": "Registra apenas as chaves das observações utilizadas em cada dia local, separando instantes e intervalos."})

            if layer == "source" and file:
                client = {"municipalities": ("ibge", "get_municipalities"), "geocoding": ("geocoding", "get_coordinates"), "open_meteo_daily": ("open_meteo", "get_weather")}[dataset]
                methods.append({"name": client[1], "file": "pipeline/consume APIs/" + client[0] + ".py", "description": "Consulta a API da fonte e valida a resposta antes da gravação."})

            if layer == "source":
                checks = ["Os arquivos originais permanecem na pasta da fonte; a prévia de registros é a leitura da Bronze."]
            elif layer == "bronze":
                checks = ["Entrada e destino precisam ser pastas separadas.", "Leitura com contrato de estrutura e interrupção em formato incompatível.", "Gravação temporária e conferência antes de substituir a saída."]

                if dataset in {"inmet_daily", "inmet_boundary"}:
                    checks.append("Cabeçalho, unidades e metadados da estação conferidos antes de interpretar as medições.")
            elif layer == "load":
                checks = ["Schema e chaves da Gold conferidos.", "Publicação das cinco tabelas e dos vínculos na mesma transação.", "Mudança dos arquivos durante a publicação interrompe e desfaz a transação."]
            else:
                checks = list(CHECKS.get(dataset, CHECKS["inmet_daily"]))

                if layer == "silver" and dataset in {"inmet_daily", "inmet_boundary"}:
                    checks = checks[:3]
                elif layer == "gold" and dataset in {"inmet_daily", "inmet_boundary"}:
                    checks = checks[3:]

            if layer == "silver" and dataset == "municipalities":
                checks = ["Campos obrigatórios preenchidos e IDs válidos.", "Coerência entre ID do município, UF e hierarquias do IBGE.", "Duplicatas equivalentes consolidadas; dados conflitantes interrompem a etapa."]
            elif layer == "silver" and dataset == "geocoding":
                checks = ["ID, nome e UF preenchidos e válidos.", "Latitude entre -90 e 90 e longitude entre -180 e 180.", "Duplicatas equivalentes consolidadas; coordenadas conflitantes interrompem a etapa."]
            elif layer == "silver" and dataset == "population":
                checks = ["Seis células por linha e data de referência interpretável no título.", "Códigos, nomes, população positiva e origens válidos.", "Conflitos para o mesmo município e referência interrompem a etapa."]
            elif layer == "gold" and dataset == "municipalities":
                checks = ["As 27 capitais oficiais precisam estar presentes, sem IDs repetidos.", "ID, nome, UF e região conferidos com a referência oficial."]
            elif layer == "gold" and dataset == "population":
                checks = ["Cada referência precisa ter estimativas válidas das capitais.", "Unicidade por município e data; nome e UF coerentes com o cadastro oficial.", "Referências diferentes são preservadas separadamente."]

            # Identifica a etapa e o ponto de chamada sem enviar código à interface.
            stage_name = {"source": "Fonte / coleta", "bronze": "Bronze", "silver": "Silver", "gold": "Gold", "load": "Load / PostgreSQL"}[layer]
            orchestrator = "run_consume" if layer == "source" else "run_pipeline" if layer == "load" else "run_" + layer

            for method in methods:
                method["stage"] = stage_name

                if method["name"] == function:
                    method["role"] = "Função principal da etapa"
                    method["used_by"] = "pipeline/run_pipeline.py :: " + orchestrator
                    method["usage"] = {
                        "source": "Executada na coleta, antes da Bronze. Consulta as APIs e prepara os arquivos de entrada das capitais.",
                        "bronze": "Executada depois da obtenção dos arquivos. Lê a fonte bruta e entrega o Parquet que será consumido pela Silver.",
                        "silver": "Executada depois da Bronze. Padroniza os registros preservados e entrega a entrada tratada para a Gold.",
                        "gold": "Executada depois da Silver e das referências necessárias. Prepara os registros finais que serão publicados pelo Load.",
                        "load": "Executada depois da Gold. Publica as cinco tabelas e seus vínculos no PostgreSQL na mesma transação.",
                    }[layer]
                elif method["name"] == "write_parquet":
                    method["role"] = "Gravação compartilhada"
                    method["used_by"] = file + " :: " + function
                    method["usage"] = "Chamada ao final do processamento para publicar o Parquet deste nó."

                    if layer == "bronze":
                        method["used_by"] += " → pipeline/bronze/bronze_storage.py :: write_bronze"
                        method["usage"] = "A função de ingestão chama write_bronze, que delega a gravação segura a write_parquet."
                elif method["name"] == "validate_quality":
                    method["role"] = "Validação de qualidade"
                    method["used_by"] = file + " :: " + function
                    method["usage"] = "Confere a coerência da entrada Silver antes de gerar os indicadores da Gold; não corrige os valores originais."
                elif method["name"] == "write_inmet_links":
                    method["role"] = "Registro de proveniência"
                    method["used_by"] = file + " :: " + function
                    method["usage"] = "Depois de gravar a Gold INMET, registra as chaves das horas associadas a cada capital e dia, para o Load publicar o caminho."
                else:
                    method["role"] = "Cliente da API"
                    method["used_by"] = "pipeline/consume APIs/consume.py :: consume"
                    method["usage"] = "Usada durante a coleta desta fonte, antes de salvar os JSONs que serão lidos pela Bronze."

            nodes.append({
                "id": node_id,
                "dataset": dataset,
                "source": source,
                "title": title,
                "origin": origin,
                "layer": layer,
                "path": path,
                "file": file,
                "description": description,
                "methods": methods,
                "checks": checks,
                "stage_name": stage_name,
                "check_function": function,
                "parents": parents,
                "key_fields": key_fields,
                "row": row,
                "column": column,
            })

    return deepcopy(nodes)


def find_node(node_id, year="2025"):
    for node in catalog(year):
        if node["id"] == node_id:
            return node

    raise ValueError("Nó desconhecido.")
