import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import { test } from "node:test";

const source = fs
  .readFileSync(new URL("../src/app.js", import.meta.url), "utf8")
  .replace(/\r\n/g, "\n");

function pollContext() {
  const configured = [];
  const selected = [];
  const context = vm.createContext({
    progressGeneration: 1,
    pipelineState: { execution: { run_id: "run", status: "running" } },
    document: { hidden: false, querySelector: () => ({ classList: { add() {}, remove() {} } }) },
    loadPipeline: async () => ({ execution: { run_id: "run", status: "succeeded" }, nodes: [] }),
    api: {
      get: async (path) =>
        path === "/overview" ? { tag: "current" } : { items: [{ municipality_id: 1 }] },
    },
    buildNodes: () => [],
    details: { configure() {} },
    graph: { highlightTrace() {}, render() {} },
    map: {
      selected: { municipality_id: 1 },
      date: "2025-01-01",
      configure: async (_capitals, overview) => configured.push(overview.tag),
      select: (capital) => selected.push(capital),
    },
    scheduleProgress() {},
    selectedNode: null,
  });
  const start = source.indexOf("async function pollProgress(");
  const end = source.indexOf("async function search", start);

  assert(start >= 0 && end > start);
  vm.runInContext(source.slice(start, end), context);

  return { context, configured, selected };
}

test("a publicação atual atualiza o mapa e preserva a seleção", async () => {
  const { context, configured, selected } = pollContext();

  await context.pollProgress(1);
  assert.equal(context.overview.tag, "current");
  assert.deepEqual(configured, ["current"]);
  assert.equal(selected.length, 1);
});

test("uma resposta antiga não substitui o mapa após atualizar a conexão", async () => {
  const { context, configured } = pollContext();
  let finish;

  context.api.get = (path) =>
    path === "/overview"
      ? new Promise((resolve) => {
          finish = resolve;
        })
      : Promise.resolve({ items: [{ municipality_id: 1 }] });
  const pending = context.pollProgress(1);

  await new Promise(setImmediate);
  context.progressGeneration = 2;
  context.overview = { tag: "fresh" };
  context.capitals = [{ municipality_id: 2 }];
  finish({ tag: "old" });
  await pending;
  assert.equal(context.overview.tag, "fresh");
  assert.equal(context.capitals[0].municipality_id, 2);
  assert.deepEqual(configured, []);
});

test("uma configuração antiga não restaura a seleção após novo refresh", async () => {
  const { context, selected } = pollContext();
  let finish;

  context.map.configure = () =>
    new Promise((resolve) => {
      finish = resolve;
    });
  const pending = context.pollProgress(1);

  await new Promise(setImmediate);
  context.progressGeneration = 2;
  finish();
  await pending;
  assert.equal(selected.length, 0);
});

test("uma falha antiga no autocomplete preserva as sugestões mais recentes", async () => {
  let handler;
  let rejectOld;
  let calls = 0;
  const timers = [];
  const rows = [];
  const input = {
    value: "Rec",
    addEventListener: (_event, callback) => {
      handler = callback;
    },
  };
  const context = vm.createContext({
    document: { querySelector: () => input },
    clearTimeout() {},
    setTimeout: (callback) => {
      timers.push(callback);

      return timers.length;
    },
    suggestionsBox: {
      replaceChildren: () => {
        rows.length = 0;
      },
      append: (row) => rows.push(row),
    },
    element: (_tag, _attributes, label) => label,
    search() {},
    api: {
      get: () => {
        calls += 1;

        if (calls === 1) {
          return new Promise((_resolve, reject) => {
            rejectOld = reject;
          });
        }

        if (calls === 3) {
          return Promise.reject(new Error("Falha atual"));
        }

        return Promise.resolve({
          items: [{ name: "Recife", state_code: "PE", municipality_id: 2611606 }],
        });
      },
    },
  });
  const start = source.indexOf("let suggestionTimer;");
  const end = source.indexOf('\ndocument.addEventListener("click"', start);

  assert(start >= 0 && end > start);
  vm.runInContext(source.slice(start, end), context);
  handler();
  const pending = timers.shift()();

  input.value = "Recife";
  handler();
  await timers.shift()();
  assert.deepEqual(rows, ["Recife · PE"]);
  rejectOld(new Error("Falha antiga"));
  await pending;
  assert.deepEqual(rows, ["Recife · PE"]);
  assert.equal(input.value, "Recife");
  handler();
  await timers.shift()();
  assert.deepEqual(rows, []);
});
