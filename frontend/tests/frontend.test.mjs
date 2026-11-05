import assert from "node:assert/strict";
import { test } from "node:test";
import { Api } from "../src/api.js";
import {
  buildNodes,
  PipelineGraph,
  graphConnections,
  loadPipeline,
  nodeStatus,
} from "../src/pipeline.js";
import { DetailsPanel } from "../src/details.js";
import { format } from "../src/ui.js";

test("valores ausentes, zero e texto vazio permanecem distintos", () => {
  assert.equal(format(null), "—");
  assert.equal(format(0, "mm"), "0 mm");
  assert.equal(format(""), '""');
});

test("a API codifica parâmetros e rejeita protocolos sem HTTP", () => {
  assert.throws(() => new Api("javascript:alert(1)"));
  assert.throws(() => new Api("http://usuario:senha@localhost"));
  const api = new Api("http://127.0.0.1:8000");
  const url = api.url("/datasets/geocoding/records", {
    state_code: "SP",
    name: "São Paulo&x=1",
    empty: null,
  });

  assert.equal(url.searchParams.get("name"), "São Paulo&x=1");
  assert.equal(url.searchParams.has("x"), false);
  assert.equal(url.searchParams.has("empty"), false);
});

test("erros HTTP preservam a mensagem e o status do backend", async (t) => {
  t.mock.method(globalThis, "fetch", async () => ({
    ok: false,
    status: 409,
    json: async () => ({ detail: "Integridade inválida" }),
  }));
  await assert.rejects(
    new Api("http://localhost:8000").get("/pipeline/nodes/gold:municipalities/records"),
    (error) => error.status === 409 && error.message === "Integridade inválida",
  );
});

test("uma busca cancelada não fica pendurada", async (t) => {
  const controller = new AbortController();

  controller.abort();
  t.mock.method(globalThis, "fetch", async (_url, options) => {
    assert.equal(options.signal.aborted, true);

    throw new DOMException("Aborted", "AbortError");
  });
  await assert.rejects(
    new Api("http://localhost:8000").get("/health", {}, controller.signal),
    /cancelada/,
  );
});

test("o grafo representa junções explícitas sem inventar conexões", () => {
  const nodes = [
    { id: "silver:weather", parents: [] },
    { id: "gold:geo", parents: [] },
    { id: "gold:weather", parents: ["silver:weather", "gold:geo"] },
  ];

  assert.equal(graphConnections(nodes).length, 2);
  assert.equal(buildNodes({ nodes }), nodes);
  assert.equal(nodeStatus({ status: "succeeded" }), "Concluída");
});

test("a tela consulta somente a execução atual", async () => {
  const calls = [];
  const result = { execution: { status: "succeeded" }, nodes: [] };
  const api = {
    get: async (path) => {
      calls.push(path);

      return result;
    },
  };

  assert.equal(await loadPipeline(api), result);
  assert.deepEqual(calls, ["/pipeline"]);
});

test("uma pesquisa única abre o caminho sem exigir Rastrear", async () => {
  const record = {
    node_id: "gold:municipalities",
    key: { municipality_id: 2611606 },
    values: { name: "Recife" },
  };
  const calls = [];
  const context = {
    query: "recfe",
    request: 1,
    pathBox: {},
    api: {
      get: async (path, parameters) => {
        assert.equal(parameters.q, "recfe");
        assert.equal(parameters.limit, 2);

        return { items: [record], has_more: false, suggestions: [] };
      },
    },
    trace: async (item) => calls.push(item),
  };

  await DetailsPanel.prototype.searchPath.call(context, { id: record.node_id });
  assert.deepEqual(calls, [record]);
});

test("uma resposta de pesquisa antiga não abre o caminho no novo nó", async () => {
  let traced = false;
  const context = {
    query: "Recife",
    request: 1,
    pathBox: {},
    api: {
      get: async () => {
        context.request += 1;

        return { items: [{}], has_more: false };
      },
    },
    trace: async () => {
      traced = true;
    },
  };

  await DetailsPanel.prototype.searchPath.call(context, { id: "gold:municipalities" });
  assert.equal(traced, false);
});

test("o destaque usa somente conexões reais, mesmo com os dois nós no percurso", () => {
  const wires = [];

  for (const target of ["b", "c"]) {
    const wire = {
      dataset: { source: "a", target, layer: "silver" },
      selected: false,
      marker: "",
      classList: {
        toggle: (_name, selected) => {
          wire.selected = selected;
        },
      },
      setAttribute: (_name, value) => {
        wire.marker = value;
      },
    };

    wires.push(wire);
  }

  const graph = {
    container: {
      classList: { toggle() {} },
      querySelectorAll: (selector) => (selector === ".wire" ? wires : []),
    },
  };

  PipelineGraph.prototype.highlightTrace.call(
    graph,
    ["a", "b", "c"],
    [{ source: "a", target: "b" }],
  );
  assert.equal(wires[0].selected, true);
  assert.equal(wires[0].marker, "url(#arrow-trace)");
  assert.equal(wires[1].selected, false);
});
