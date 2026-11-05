import { element, format, message, table, jsonBlock, tabbedContent, dateTime } from "./ui.js";

function keyLabel(key) {
  const fields = [];

  for (const [name, value] of Object.entries(key)) {
    fields.push(name + "=" + String(value));
  }

  return fields.join(" · ");
}

export class DetailsPanel {
  constructor(api, onClose, onTrace, onRecord, onNode) {
    this.api = api;
    this.onClose = onClose;
    this.onTrace = onTrace;
    this.onRecord = onRecord;
    this.onNode = onNode;
    this.container = document.querySelector("#details-panel");
    this.request = 0;
    this.journey = null;
  }

  configure(nodes) {
    this.nodes = nodes;
    this.close();
  }

  close() {
    this.request += 1;
    this.container.hidden = true;
    document.querySelector("#pipeline-workspace").classList.remove("with-panel");
  }

  async open(node, filters = {}) {
    this.node = node;
    this.query = filters.q || (filters.municipality_id ? String(filters.municipality_id) : "");
    const ticket = ++this.request;

    this.container.hidden = false;
    document.querySelector("#pipeline-workspace").classList.add("with-panel");
    const content = element("div", { className: "panel-content" });

    this.container.replaceChildren(
      element(
        "div",
        { className: "panel-heading" },
        element("span", { className: "eyebrow" }, node.layer.toUpperCase() + " · DETALHES DO NÓ"),
        element(
          "button",
          { className: "close-button", "aria-label": "Fechar detalhes", onClick: this.onClose },
          "×",
        ),
        element("h2", {}, node.title),
      ),
      content,
    );
    const definitions = [
      {
        key: "summary",
        label: "Resumo",
        load: async () => {
          let count = node.records;

          if (node.layer !== "source") {
            count = (await this.api.get(`/pipeline/nodes/${node.id}`)).records;
          }

          return element(
            "div",
            {},
            element("p", {}, node.description),
            element(
              "div",
              { className: "stat-grid" },
              element(
                "div",
                {},
                element("span", {}, node.layer === "source" ? "Arquivos" : "Registros"),
                element("strong", {}, format(node.layer === "source" ? node.files_count : count)),
              ),
              element(
                "div",
                {},
                element("span", {}, "Duração"),
                element("strong", {}, format(node.execution?.duration_seconds, "s")),
              ),
            ),
            element("p", { className: "break-word small" }, node.file),
            element(
              "p",
              { className: "small" },
              "Última conclusão: " + dateTime(node.execution?.finished_at),
            ),
            element("h3", {}, "Entradas desta etapa"),
            this.parents(node),
          );
        },
      },
      { key: "records", label: "Registros", load: () => this.records(node) },
      {
        key: "columns",
        label: "Colunas",
        load: async () => table((await this.api.get(`/pipeline/nodes/${node.id}`)).columns),
      },
      { key: "methods", label: "Métodos", load: async () => this.methods(node) },
      { key: "checks", label: "Verificações", load: async () => this.checks(node) },
      {
        key: "path",
        label: "Caminho",
        load: async () => {
          this.pathBox = element("div");

          if (this.journey) {
            this.renderJourney(this.journey, this.pathBox);
          } else if (this.record) {
            await this.trace(this.record);
          } else {
            await this.searchPath(node);
          }

          return this.pathBox;
        },
      },
    ];

    if (node.id === "gold:inmet_daily" || node.id === "load:inmet_daily") {
      definitions.splice(5, 0, {
        key: "aggregation",
        label: "Agregação",
        load: async () => {
          const box = element(
            "div",
            {},
            element("h3", {}, "Das horas ao dia local"),
            element(
              "p",
              {},
              "Cada linha representa uma capital e uma data. A estação é fixa no período: mesma UF, distância de até 50 km e cobertura mínima de 90% de temperatura e chuva.",
            ),
            table([
              {
                indicador: "Temperatura média",
                operação: "Média das temperaturas instantâneas",
                requisito: "Ao menos 90% das horas",
              },
              {
                indicador: "Temperatura mínima / máxima",
                operação: "Extremo dos intervalos horários",
                requisito: "100% das horas",
              },
              {
                indicador: "Chuva",
                operação: "Soma dos acumulados horários",
                requisito: "100% das horas",
              },
              {
                indicador: "Vento máximo",
                operação: "Maior velocidade instantânea",
                requisito: "100% das horas",
              },
              {
                indicador: "Rajada máxima",
                operação: "Maior rajada dos intervalos horários",
                requisito: "100% das horas",
              },
            ]),
            element(
              "p",
              {},
              "Intervalos pertencem à hora encerrada; instantes pertencem à data local da mensagem. A borda UTC completa a virada do ano quando compatível. Dias sem cobertura suficiente mantêm o indicador nulo.",
            ),
            element(
              "p",
              { className: "small" },
              "Use Registros e Caminho para consultar a estação, as coberturas e as horas efetivamente associadas ao dia pesquisado.",
            ),
          );

          return box;
        },
      });
    }

    if (node.layer === "source") {
      definitions.push({ key: "files", label: "Arquivos", load: () => this.files(node) });
    }

    this.record = filters.record || null;
    this.journey = filters.journey || null;
    this.pathBox = null;
    this.tabs = tabbedContent(
      definitions,
      filters.focusTab || (filters.focusPath || this.query ? "path" : "summary"),
    );

    if (ticket === this.request) {
      content.replaceChildren(this.tabs);
    }
  }

  async searchPath(node) {
    const box = this.pathBox;
    const ticket = this.request;
    const query = this.query;
    const searchTicket = (this.pathSearchRequest || 0) + 1;

    this.pathSearchRequest = searchTicket;

    const isCurrent = () =>
      ticket === this.request &&
      query === this.query &&
      box === this.pathBox &&
      searchTicket === this.pathSearchRequest;

    if (!query) {
      box.replaceChildren(
        element(
          "p",
          { className: "info-note" },
          "Pesquise um dado para ver seu caminho aqui. Combine a capital com a data quando houver vários registros; também é possível escolher Rastrear na aba Registros.",
        ),
      );

      return;
    }

    let result;

    try {
      result = await this.api.get(`/pipeline/nodes/${node.id}/records`, {
        q: query,
        limit: 2,
      });
    } catch (error) {
      if (!isCurrent()) {
        return;
      }

      throw error;
    }

    if (!isCurrent()) {
      return;
    }

    if (result.items.length === 1 && !result.has_more && !result.suggestions?.length) {
      this.record = result.items[0];
      await this.trace(this.record);

      if (isCurrent() && result.message) {
        box.prepend(element("p", { className: "small" }, result.message));
      }

      return;
    }

    await this.records(node, 0, box);

    if (isCurrent() && result.items.length) {
      box.prepend(
        element(
          "p",
          { className: "info-note" },
          "Há mais de um registro para esta pesquisa. Escolha Rastrear em uma linha ou acrescente uma data para acompanhar um dado específico.",
        ),
      );
    }
  }

  parents(node) {
    const box = element("div");

    for (const id of node.parents || []) {
      const parent = this.nodes.find((item) => item.id === id);

      box.append(element("p", {}, `${parent?.title || id} · ${parent?.layer || ""}`));
    }

    if (!node.parents?.length) {
      box.append(element("p", {}, "Origem dos dados."));
    }

    return box;
  }

  stageNavigation(node, tab) {
    const box = element("div", { className: "stage-navigation" });
    const journeyIds = this.journey ? new Set(this.journey.nodes.map((item) => item.id)) : null;
    const stages = (this.nodes || []).filter((item) =>
      journeyIds ? journeyIds.has(item.id) : item.dataset === node.dataset,
    );

    box.append(
      element(
        "p",
        { className: "small" },
        "As informações abaixo pertencem ao nó selecionado. Escolha outra etapa para ver suas funções e verificações.",
      ),
    );

    if (!this.onNode || !stages.length) {
      return box;
    }

    const selector = element("select", {
      onChange: (event) => {
        const target = stages.find((item) => item.id === event.target.value);

        if (target) {
          this.onNode(target, { q: this.query, journey: this.journey, focusTab: tab });
        }
      },
    });

    for (const stage of stages) {
      const option = element(
        "option",
        { value: stage.id },
        (stage.stage_name || stage.layer.toUpperCase()) + " · " + stage.title,
      );

      option.selected = stage.id === node.id;
      selector.append(option);
    }

    box.append(
      element(
        "label",
        {},
        journeyIds ? "Etapas do dado pesquisado" : "Etapas desta fonte",
        selector,
      ),
    );

    return box;
  }

  checks(node) {
    const stage = node.stage_name || node.layer.toUpperCase();
    const box = element(
      "div",
      {},
      this.stageNavigation(node, "checks"),
      element("h3", {}, stage + " · " + node.title),
    );

    if (node.check_function) {
      box.append(
        element(
          "div",
          { className: "check-owner" },
          element("span", { className: "eyebrow" }, "Responsável pelas verificações"),
          element("strong", { className: "break-word" }, node.check_function + "()"),
          element("span", { className: "small break-word" }, node.file),
        ),
      );
    }

    box.append(
      element(
        "p",
        { className: "small" },
        node.layer === "source"
          ? "Esta etapa identifica a origem dos arquivos. As verificações de leitura e transformação aparecem nos nós Bronze, Silver e Gold."
          : "Regras previstas no código deste nó, na etapa " +
              stage +
              ". O estado da execução aparece no Resumo; esta lista descreve as regras, não um laudo individual do registro pesquisado.",
      ),
    );

    for (const [index, check] of (node.checks || []).entries()) {
      box.append(
        element(
          "section",
          { className: "check-card" },
          element(
            "span",
            { className: "check-number", "aria-hidden": "true" },
            String(index + 1).padStart(2, "0"),
          ),
          element(
            "div",
            {},
            element("span", { className: "eyebrow" }, "Etapa: " + stage),
            element("p", {}, typeof check === "string" ? check : JSON.stringify(check)),
          ),
        ),
      );
    }

    if (!node.checks?.length) {
      box.append(element("p", {}, "Nenhuma verificação adicional foi descrita para este nó."));
    }

    return box;
  }

  methods(node) {
    const box = element(
      "div",
      {},
      this.stageNavigation(node, "methods"),
      element("h3", {}, (node.stage_name || node.layer.toUpperCase()) + " · " + node.title),
    );

    for (const method of node.methods || []) {
      const context = element("dl", { className: "method-context" });

      for (const [label, value] of [
        ["Etapa", method.stage || node.stage_name || node.layer.toUpperCase()],
        ["Definida em", method.file || node.file],
        ["Usada por", method.used_by || node.file],
      ]) {
        context.append(
          element(
            "div",
            { className: "method-detail" },
            element("dt", {}, label),
            element("dd", { className: "break-word" }, value),
          ),
        );
      }

      box.append(
        element(
          "details",
          { className: "method-card" },
          element(
            "summary",
            { className: "method-heading" },
            element("span", { className: "eyebrow" }, method.role || "Função da etapa"),
            element("h4", { className: "break-word" }, (method.name || method.function) + "()"),
          ),
          element(
            "div",
            { className: "method-description" },
            element("h5", {}, "O que faz"),
            element("p", {}, method.description),
          ),
          context,
          element(
            "div",
            { className: "method-usage" },
            element("h5", {}, "Quando é usada"),
            element("p", {}, method.usage || "Utilizada no processamento deste nó."),
          ),
        ),
      );
    }

    if (!node.methods?.length) {
      const bronze = (this.nodes || []).find((item) => item.id === "bronze:" + node.dataset);

      box.append(
        element(
          "details",
          { className: "method-card" },
          element(
            "summary",
            { className: "method-heading" },
            element("span", { className: "eyebrow" }, "Fonte manual"),
            element("h4", {}, "Obtenção do arquivo"),
          ),
          element(
            "div",
            { className: "method-description" },
            element(
              "p",
              {},
              "O arquivo foi obtido manualmente e colocado na pasta de entrada. Não há consulta de API para esta fonte.",
            ),
            element("p", { className: "small break-word" }, "Pasta: data/" + node.path),
          ),
          element(
            "div",
            { className: "method-usage" },
            element("h5", {}, "Onde começa o processamento automático"),
            element("p", {}, "Na Bronze, que lê e preserva os dados originais."),
            bronze
              ? element(
                  "p",
                  { className: "small break-word" },
                  bronze.check_function + "() · " + bronze.file,
                )
              : null,
          ),
        ),
      );
    }

    return box;
  }

  async records(node, offset = 0, box = element("div")) {
    const ticket = this.request;
    const query = this.query;
    const pageRequest = {};

    this.recordRequests ??= new WeakMap();
    this.recordRequests.set(box, pageRequest);

    const isCurrent = () =>
      ticket === this.request &&
      query === this.query &&
      this.recordRequests.get(box) === pageRequest;
    let result;

    try {
      result = await this.api.get(`/pipeline/nodes/${node.id}/records`, {
        q: query,
        limit: 10,
        offset,
      });
    } catch (error) {
      if (!isCurrent()) {
        return box;
      }

      throw error;
    }

    if (!isCurrent()) {
      return box;
    }

    const rows = [];
    const originals = new Map();

    for (const item of result.items) {
      rows.push(item.values);
      originals.set(item.values, item);
    }

    box.replaceChildren(
      element(
        "p",
        { className: "small" },
        result.message || (this.query ? "Filtro: " + this.query : "Dados atuais desta etapa."),
      ),
      table(rows, (row) => {
        this.record = originals.get(row);
        const alreadyLoaded = Boolean(this.pathBox);

        this.tabs.querySelector('[data-tab="path"]').click();

        if (alreadyLoaded) {
          this.trace(this.record);
        }
      }),
    );

    for (const suggestion of result.suggestions || []) {
      box.append(
        element(
          "button",
          {
            className: "button",
            onClick: () => {
              this.query = suggestion.query || String(suggestion.municipality_id);
              const update =
                box === this.pathBox ? this.searchPath(node) : this.records(node, 0, box);

              update.catch((error) =>
                box.replaceChildren(message("Consulta indisponível", error.message)),
              );
            },
          },
          suggestion.name + " · " + suggestion.state_code,
        ),
      );
    }

    const controls = element("div", { className: "record-pagination" });
    const page = (next) =>
      this.records(node, next, box).catch((error) =>
        box.replaceChildren(message("Consulta indisponível", error.message)),
      );

    controls.append(
      element(
        "button",
        {
          className: "button",
          disabled: offset === 0,
          onClick: () => page(Math.max(0, offset - 10)),
        },
        "Anterior",
      ),
      element("span", {}, "Página " + (offset / 10 + 1)),
      element(
        "button",
        {
          className: "button",
          disabled: !result.has_more,
          onClick: () => page(result.next_offset),
        },
        "Próxima",
      ),
    );
    box.append(controls);

    return box;
  }

  async files(node) {
    const result = await this.api.get(`/pipeline/nodes/${node.id}/files`);
    const box = element("div");

    for (const file of result.items) {
      box.append(
        element(
          "p",
          {},
          element(
            "a",
            {
              href: this.api.base + "/pipeline/file?path=" + encodeURIComponent(file.path),
              download: file.name,
            },
            file.path,
          ),
        ),
      );
    }

    return box;
  }

  async search(query, cursor = {}, previous = []) {
    const ticket = ++this.request;

    this.query = query;
    this.journey = null;
    this.container.hidden = false;
    document.querySelector("#pipeline-workspace").classList.add("with-panel");
    const content = element("div", { className: "panel-content" });

    this.container.replaceChildren(
      element(
        "div",
        { className: "panel-heading" },
        element("span", { className: "eyebrow" }, "PESQUISA EM TODAS AS CAMADAS"),
        element(
          "button",
          { className: "close-button", "aria-label": "Fechar pesquisa", onClick: this.onClose },
          "×",
        ),
        element("h2", {}, "Resultados da pesquisa"),
      ),
      content,
    );
    content.append(
      message(
        "Pesquisando",
        "Consultando as camadas, inclusive registros fora da Gold…",
        null,
        true,
      ),
    );

    try {
      const page = await this.api.get("/pipeline/search", { q: query, limit: 20, ...cursor });

      if (ticket !== this.request) {
        return;
      }

      if (
        page.items.length === 1 &&
        !page.has_more &&
        !page.suggestions?.length &&
        !previous.length
      ) {
        this.onRecord(page.items[0]);

        return;
      }

      content.replaceChildren(
        element(
          "p",
          { className: "small" },
          page.message ||
            "Escolha um registro para mostrar seu caminho completo. Pesquisa: " + query,
        ),
      );

      for (const suggestion of page.suggestions || []) {
        content.append(
          element(
            "button",
            {
              className: "button",
              onClick: () => this.search(suggestion.query || String(suggestion.municipality_id)),
            },
            suggestion.name + " · " + suggestion.state_code,
          ),
        );
      }

      for (const record of page.items) {
        const node = this.nodes.find((item) => item.id === record.node_id);
        const values = record.values;

        content.append(
          element(
            "section",
            { className: "info-note search-result" },
            element(
              "strong",
              {},
              values.name ||
                values.nome ||
                values.station_name ||
                values.station_code ||
                node.title,
            ),
            element(
              "p",
              { className: "small break-word" },
              `${node.title} · ${node.layer} · ${keyLabel(record.key)}`,
            ),
            element(
              "button",
              { className: "text-button", onClick: () => this.onRecord(record) },
              "Ver caminho completo →",
            ),
          ),
        );
      }

      if (!page.items.length && !page.suggestions?.length) {
        content.append(
          element(
            "p",
            { className: "info-note" },
            "Nenhum registro encontrado. Este projeto não contém etapas de treino nem agregados mensais; experimente os campos disponíveis nas abas Colunas.",
          ),
        );
      }

      content.append(
        element(
          "div",
          { className: "record-pagination" },
          element(
            "button",
            {
              className: "button",
              disabled: !previous.length,
              onClick: () => this.search(query, previous.at(-1), previous.slice(0, -1)),
            },
            "Anterior",
          ),
          element(
            "button",
            {
              className: "button",
              disabled: !page.has_more,
              onClick: () => this.search(query, page.next_cursor, [...previous, cursor]),
            },
            "Próxima",
          ),
        ),
      );
    } catch (error) {
      if (ticket === this.request) {
        content.replaceChildren(message("Pesquisa indisponível", error.message));
      }
    }
  }

  async trace(record) {
    const ticket = this.request;
    const box = this.pathBox;

    if (!box) {
      return;
    }

    const traceTicket = (this.traceRequest || 0) + 1;

    this.traceRequest = traceTicket;
    this.journey = null;
    this.record = record;
    this.onTrace([], []);
    box.replaceChildren(
      message(
        "Calculando percurso completo",
        "Reunindo origens, junções, agregações e destinos reais…",
        null,
        true,
      ),
    );

    try {
      const result = await this.api.get("/pipeline/journey", {
        node_id: record.node_id,
        key: JSON.stringify(record.key),
      });

      if (ticket !== this.request || traceTicket !== this.traceRequest) {
        return;
      }

      this.record = result.record;
      this.journey = result;
      this.renderJourney(result, box);
      const identifiers = [];

      for (const node of result.nodes) {
        identifiers.push(node.id);
      }

      this.onTrace(identifiers, result.edges);
    } catch (error) {
      if (ticket === this.request && traceTicket === this.traceRequest) {
        box.replaceChildren(message("Caminho indisponível", error.message));
      }
    }
  }

  renderJourney(result, box) {
    const record = result.record;

    box.replaceChildren(
      element("h3", {}, "Caminho completo do dado"),
      element(
        "p",
        { className: "info-note" },
        record.values.name ||
          record.values.nome ||
          record.values.station_name ||
          record.values.station_code ||
          "Registro pesquisado",
      ),
      element("p", { className: "small break-word" }, keyLabel(record.key)),
      element(
        "p",
        { className: "small" },
        `${result.nodes.length} nós · ${format(result.records_count)} registros participantes. O grafo destaca somente as conexões reais deste percurso.`,
      ),
    );

    for (const group of result.nodes) {
      const node = this.nodes.find((item) => item.id === group.id);
      const section = element(
        "details",
        { className: "journey-stage" },
        element(
          "summary",
          {},
          `${node.title} · ${node.layer} · ${format(group.records_count)} registro(s)`,
        ),
      );

      for (const example of group.examples) {
        section.append(
          element("p", { className: "small break-word" }, keyLabel(example.key)),
          jsonBlock(example.values),
        );
      }

      if (group.records_count > group.examples.length) {
        section.append(
          element(
            "p",
            { className: "small" },
            "Exemplos de participantes. A contagem e o destaque consideram todos os vínculos, não somente esta amostra.",
          ),
        );
      }

      box.append(section);
    }

    for (const stop of result.stops) {
      const node = this.nodes.find((item) => item.id === stop.node_id);

      box.append(
        element("p", { className: "info-note" }, `${node.title} · ${node.layer}: ${stop.reason}`),
      );
    }
  }
}
