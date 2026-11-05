import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import { test } from "node:test";
import { DetailsPanel } from "../src/details.js";

const source = fs.readFileSync(new URL("../src/app.js", import.meta.url), "utf8");

function element(tag, attributes = {}, ...children) {
  return {
    tag,
    attributes,
    children,
    append(...items) {
      this.children.push(...items);
    },
  };
}

test("a pesquisa fica disponível antes de abrir o resultado único", async () => {
  const record = { node_id: "gold:open_meteo_daily", key: { municipality_id: 2611606 } };
  const search = vm.runInNewContext(
    "(" +
      DetailsPanel.prototype.search.toString().replace(/^async search\(/, "async function (") +
      ")",
    {
      element,
      message: () => element("div"),
      document: { querySelector: () => ({ classList: { add() {} } }) },
    },
  );
  const query = "Recife;17/03/2025";
  const context = {
    request: 0,
    container: { replaceChildren() {} },
    api: { get: async () => ({ items: [record], has_more: false, suggestions: [] }) },
    onRecord(item) {
      assert.equal(context.query, query);
      assert.equal(item, record);
    },
  };

  await search.call(context, query);
  assert.equal(context.query, query);
});

test("abrir o resultado e trocar de nó preserva o filtro dos Registros", async () => {
  const calls = [];
  class Panel {
    constructor(_api, _close, _trace, onRecord) {
      this.onRecord = onRecord;
      this.query = "Recife;17/03/2025";
    }

    open(node, filters) {
      calls.push({ node, filters });
    }
  }
  const context = vm.createContext({
    api: {},
    DetailsPanel: Panel,
    closeDetails() {},
    notice() {},
    selectedNode: null,
    nodes: [{ id: "gold:open_meteo_daily" }, { id: "gold:municipalities" }],
    graph: { render() {}, highlightTrace() {} },
  });
  const declaration = source.slice(
    source.indexOf("const details = new DetailsPanel("),
    source.indexOf("const map ="),
  );
  const select = source.slice(
    source.indexOf("function selectNode("),
    source.indexOf("function updateTheme("),
  );

  vm.runInContext(select + declaration, context);
  vm.runInContext('details.onRecord({ node_id: "gold:open_meteo_daily" });', context);
  assert.equal(calls[0].filters.q, "Recife;17/03/2025");
  assert.equal(calls[0].filters.focusPath, true);

  vm.runInContext("selectNode(nodes[1]);", context);
  assert.equal(calls[1].filters.q, "Recife;17/03/2025");

  const search = source.slice(
    source.indexOf("async function search("),
    source.indexOf("const suggestionsBox ="),
  );

  vm.runInContext(search, context);
  await vm.runInContext('search({ preventDefault() {} }, "")', context);
  assert.equal(calls[2].filters.q, "");
});

test("a aba Registros envia o filtro preservado ao backend", async () => {
  const calls = [];
  const records = vm.runInNewContext(
    "(" +
      DetailsPanel.prototype.records.toString().replace(/^async records\(/, "async function (") +
      ")",
    {
      element,
      table: () => element("table"),
      message: () => element("div"),
    },
  );
  const context = {
    request: 1,
    query: "Recife;17/03/2025",
    api: {
      get: async (path, parameters) => {
        calls.push({ path, parameters });
        return { items: [], suggestions: [], has_more: false };
      },
    },
  };
  const box = { replaceChildren() {}, append() {} };

  await records.call(context, { id: "gold:open_meteo_daily" }, 0, box);
  assert.equal(calls[0].parameters.q, "Recife;17/03/2025");
  assert.equal(calls[0].path, "/pipeline/nodes/gold:open_meteo_daily/records");
});

function selectionContext() {
  const journey = {
    nodes: [{ id: "source:population" }, { id: "bronze:population" }, { id: "silver:population" }],
  };
  const opened = [];
  let cleared = 0;
  const context = vm.createContext({
    details: {
      query: "Muitos Capões",
      journey,
      open(node, filters) {
        this.journey = filters.journey;
        opened.push({ node, filters });
      },
    },
    nodes: [],
    selectedNode: null,
    graph: {
      render() {},
      highlightTrace() {
        cleared += 1;
      },
    },
  });
  const select = source.slice(
    source.indexOf("function selectNode("),
    source.indexOf("function updateTheme("),
  );

  vm.runInContext(select, context);

  return { context, journey, opened, clearCount: () => cleared };
}

test("abrir um nó fora do percurso descarta o caminho anterior e mantém a pesquisa", () => {
  const { context, opened, clearCount } = selectionContext();

  context.selectNode({ id: "gold:geocoding" });

  assert.equal(opened[0].filters.journey, null);
  assert.equal(context.details.journey, null);
  assert.equal(opened[0].filters.q, "Muitos Capões");
  assert.equal(clearCount(), 1);
});

test("abrir um nó participante mantém o percurso e seu destaque", () => {
  const { context, journey, opened, clearCount } = selectionContext();

  context.selectNode({ id: "bronze:population" });

  assert.equal(opened[0].filters.journey, journey);
  assert.equal(opened[0].filters.q, "Muitos Capões");
  assert.equal(clearCount(), 0);
});

test("a checagem também vale para filtros explícitos, sem modificar o objeto recebido", () => {
  const { context, journey, opened } = selectionContext();
  const filters = { q: "Muitos Capões", journey, focusTab: "methods" };

  context.selectNode({ id: "gold:geocoding" }, filters);

  assert.equal(opened[0].filters.journey, null);
  assert.equal(opened[0].filters.focusTab, "methods");
  assert.equal(filters.journey, journey);
});
