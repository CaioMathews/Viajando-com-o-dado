import assert from "node:assert/strict";
import vm from "node:vm";
import { test } from "node:test";
import { DetailsPanel } from "../src/details.js";

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

function method(name) {
  const source = DetailsPanel.prototype[name]
    .toString()
    .replace(new RegExp("^async " + name + "\\("), "async function (");

  return vm.runInNewContext("(" + source + ")", {
    element,
    message: (title) => ({ title }),
    table: (rows) => ({ rows }),
  });
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((success, failure) => {
    resolve = success;
    reject = failure;
  });

  return { promise, resolve, reject };
}

test("rastrear dois registros rapidamente mantém o último percurso escolhido", async () => {
  const trace = method("trace");
  const first = deferred();
  const second = deferred();
  const rendered = [];
  const context = {
    request: 1,
    traceRequest: 0,
    pathBox: { replaceChildren() {} },
    api: {
      get: (_path, parameters) =>
        JSON.parse(parameters.key).id === 1 ? first.promise : second.promise,
    },
    renderJourney: (result) => rendered.push(result.record.key.id),
    onTrace() {},
  };
  const old = trace.call(context, { node_id: "silver:population", key: { id: 1 } });
  const recent = trace.call(context, { node_id: "silver:population", key: { id: 2 } });

  second.resolve({ record: { key: { id: 2 } }, nodes: [], edges: [] });
  await recent;
  first.resolve({ record: { key: { id: 1 } }, nodes: [], edges: [] });
  await old;

  assert.deepEqual(rendered, [2]);
  assert.equal(context.record.key.id, 2);
});

test("resposta atrasada de paginação não substitui a página mais recente", async () => {
  const records = method("records");
  const first = deferred();
  const box = {
    replaceChildren(...children) {
      this.children = children;
    },
    append() {},
  };
  const context = {
    request: 1,
    query: "Recife",
    api: {
      get: (_path, parameters) =>
        parameters.offset === 0
          ? first.promise
          : Promise.resolve({ items: [{ values: { name: "Página nova" } }], has_more: false }),
    },
  };
  const old = records.call(context, { id: "gold:population" }, 0, box);

  await records.call(context, { id: "gold:population" }, 10, box);
  first.resolve({ items: [{ values: { name: "Página antiga" } }], has_more: false });
  await old;

  assert.equal(box.children[1].rows[0].name, "Página nova");
});

test("uma falha antiga de rastreamento não apaga um percurso mais recente", async () => {
  const trace = method("trace");
  const first = deferred();
  const box = {
    replaceChildren(...children) {
      this.children = children;
    },
  };
  const context = {
    request: 1,
    pathBox: box,
    api: {
      get: (_path, parameters) =>
        JSON.parse(parameters.key).id === 1
          ? first.promise
          : Promise.resolve({ record: { key: { id: 2 } }, nodes: [], edges: [] }),
    },
    renderJourney: (result) => box.replaceChildren({ id: result.record.key.id }),
    onTrace() {},
  };
  const old = trace.call(context, { node_id: "silver:population", key: { id: 1 } });

  await trace.call(context, { node_id: "silver:population", key: { id: 2 } });
  first.reject(new Error("Falha antiga"));
  await old;

  assert.equal(box.children[0].id, 2);
});

test("uma falha de paginação antiga não substitui os registros já atualizados", async () => {
  const records = method("records");
  const first = deferred();
  const box = {
    replaceChildren(...children) {
      this.children = children;
    },
    append() {},
  };
  const context = {
    request: 1,
    query: "Recife",
    api: {
      get: (_path, parameters) =>
        parameters.offset === 0
          ? first.promise
          : Promise.resolve({ items: [{ values: { name: "Página nova" } }], has_more: false }),
    },
  };
  const old = records.call(context, { id: "gold:population" }, 0, box);

  await records.call(context, { id: "gold:population" }, 10, box);
  first.reject(new Error("Falha antiga"));
  await assert.doesNotReject(old);

  assert.equal(box.children[1].rows[0].name, "Página nova");
});

test("consultas de registros em abas diferentes não cancelam umas às outras", async () => {
  const records = method("records");
  const first = deferred();
  const boxes = Array.from({ length: 2 }, () => ({
    replaceChildren(...children) {
      this.children = children;
    },
    append() {},
  }));
  const context = {
    request: 1,
    query: "Recife",
    api: {
      get: (_path, parameters) =>
        parameters.offset === 0
          ? first.promise
          : Promise.resolve({ items: [{ values: { name: "Outra aba" } }], has_more: false }),
    },
  };
  const pending = records.call(context, { id: "gold:population" }, 0, boxes[0]);

  await records.call(context, { id: "gold:population" }, 10, boxes[1]);
  first.resolve({ items: [{ values: { name: "Primeira aba" } }], has_more: false });
  await pending;

  assert.equal(boxes[0].children[1].rows[0].name, "Primeira aba");
  assert.equal(boxes[1].children[1].rows[0].name, "Outra aba");
});

test("falha da consulta atual continua visível para o tratamento da interface", async () => {
  const records = method("records");
  const context = {
    request: 1,
    query: "Recife",
    api: {
      get: async () => {
        throw new Error("Fonte indisponível");
      },
    },
  };

  await assert.rejects(
    records.call(context, { id: "gold:population" }, 0, {}),
    /Fonte indisponível/,
  );
});

test("a escolha antiga de uma sugestão não rastreia outro dado após mudar a pesquisa", async () => {
  const first = deferred();
  const searchPath = method("searchPath");
  const traced = [];
  const context = {
    request: 1,
    pathBox: { replaceChildren() {} },
    query: "Recife",
    api: {
      get: (_path, parameters) =>
        parameters.q === "Recife"
          ? first.promise
          : Promise.resolve({ items: [{ key: { municipality_id: 3550308 } }], has_more: false }),
    },
    trace: async (record) => traced.push(record.key.municipality_id),
  };
  const old = searchPath.call(context, { id: "gold:municipalities" });

  context.query = "São Paulo";
  await searchPath.call(context, { id: "gold:municipalities" });
  first.resolve({ items: [{ key: { municipality_id: 2611606 } }], has_more: false });
  await old;

  assert.deepEqual(traced, [3550308]);
});
