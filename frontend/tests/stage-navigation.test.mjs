import assert from "node:assert/strict";
import { test } from "node:test";
import vm from "node:vm";
import { DetailsPanel } from "../src/details.js";

const element = (tag, attributes = {}, ...children) => ({
  tag,
  attributes,
  children: children.flat().filter((item) => item != null),
  append(...items) {
    this.children.push(...items);
  },
});
const navigate = vm.runInNewContext(
  "(" +
    DetailsPanel.prototype.stageNavigation.toString().replace(/^stageNavigation\(/, "function (") +
    ")",
  { element },
);
const nodes = [
  {
    id: "source:population",
    dataset: "population",
    layer: "source",
    stage_name: "Fonte / coleta",
    title: "População",
  },
  {
    id: "bronze:population",
    dataset: "population",
    layer: "bronze",
    stage_name: "Bronze",
    title: "População",
  },
  {
    id: "silver:population",
    dataset: "population",
    layer: "silver",
    stage_name: "Silver",
    title: "População",
  },
  {
    id: "gold:population",
    dataset: "population",
    layer: "gold",
    stage_name: "Gold",
    title: "População",
  },
  {
    id: "gold:geocoding",
    dataset: "geocoding",
    layer: "gold",
    stage_name: "Gold",
    title: "Coordenadas",
  },
];

test("a navegação inclui Fonte e Bronze do percurso, sem incluir a Gold ausente", () => {
  const journey = { nodes: nodes.slice(0, 3).map((node) => ({ id: node.id })) };
  const calls = [];
  const context = { nodes, journey, query: "Muitos Capões", onNode: (...args) => calls.push(args) };
  const box = navigate.call(context, nodes[2], "methods");
  const selector = box.children[1].children[1];

  assert.deepEqual(
    Array.from(selector.children, (option) => option.attributes.value),
    nodes.slice(0, 3).map((node) => node.id),
  );
  assert.equal(selector.children[2].selected, true);
  selector.attributes.onChange({ target: { value: "bronze:population" } });
  assert.equal(calls[0][0].id, "bronze:population");
  assert.equal(calls[0][1].q, "Muitos Capões");
  assert.equal(calls[0][1].journey, journey);
  assert.equal(calls[0][1].focusTab, "methods");
});

test("sem pesquisa, a navegação mostra somente as etapas da fonte selecionada", () => {
  const calls = [];
  const context = { nodes, journey: null, query: "", onNode: (...args) => calls.push(args) };
  const box = navigate.call(context, nodes[2], "checks");
  const selector = box.children[1].children[1];

  assert.equal(selector.children.length, 4);
  selector.attributes.onChange({ target: { value: "source:population" } });
  assert.equal(calls[0][1].focusTab, "checks");
  assert.equal(calls[0][1].journey, null);
});
const renderMethods = vm.runInNewContext(
  "(" + DetailsPanel.prototype.methods.toString().replace(/^methods\(/, "function (") + ")",
  { element },
);

test("os métodos começam recolhidos e usam um controle expansível nativo", () => {
  const node = {
    ...nodes[2],
    methods: [
      {
        name: "transform_population",
        role: "Função principal",
        stage: "Silver",
        file: "pipeline/silver/ibge_population.py",
        description: "Padroniza a população.",
        used_by: "run_silver",
        usage: "Depois da Bronze.",
      },
    ],
  };
  const box = renderMethods.call({ stageNavigation: () => null }, node);
  const card = box.children.find((child) => child.tag === "details");

  assert(card);
  assert.equal(card.attributes.open, undefined);
  assert.equal(card.children[0].tag, "summary");
  assert.equal(card.children[0].children[1].children[0], "transform_population()");
  assert.equal(card.children[2].children[2].children[1].children[0], "run_silver");
});

test("a fonte manual explica a obtenção e aponta a primeira leitura na Bronze", () => {
  const node = { ...nodes[0], path: "ibge/population", methods: [] };
  const bronze = {
    ...nodes[1],
    check_function: "ingest_population",
    file: "pipeline/bronze/ibge_population.py",
  };
  const box = renderMethods.call({ nodes: [node, bronze], stageNavigation: () => null }, node);
  const card = box.children.find((child) => child.tag === "details");

  assert.equal(card.children[0].children[1].children[0], "Obtenção do arquivo");
  assert.match(card.children[2].children[2].children[0], /ingest_population/);
});
