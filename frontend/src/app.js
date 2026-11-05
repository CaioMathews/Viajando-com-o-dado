import { Api, saveValue, storedValue } from "./api.js";
import { DetailsPanel } from "./details.js?v=20261105-details-4";
import { BrazilMap } from "./map.js";
import { buildNodes, loadPipeline, PipelineGraph } from "./pipeline.js";
import { element, message } from "./ui.js";


const api = new Api();
let pipelineState = { nodes: [], execution: {} };
let overview = null;
let capitals = [];
let nodes = [];
let selectedNode = null;
let requestController;
let activePage = "map";
let progressTimer;
let progressGeneration = 0;


const graph = new PipelineGraph(selectNode, closeDetails);
const details = new DetailsPanel(api, closeDetails, (identifiers, edges) => graph.highlightTrace(identifiers, edges), record => {
  const node = nodes.find(item => item.id === record.node_id);
  if (node) selectNode(node, { record, focusPath: true });
}, selectNode);
const map = new BrazilMap(api, capital => {
  location.hash = "pipeline";
  document.querySelector("#search-input").value = capital.name;
  const node = nodes.find(item => item.id === "gold:geocoding");
  if (node) selectNode(node, { municipality_id: capital.municipality_id });
});


function closeDetails() {
  const id = selectedNode?.id;
  selectedNode = null;
  details.close();
  graph.highlightTrace();
  graph.render(nodes);
  if (id) document.querySelector(`[data-node="${id}"]`)?.focus({ preventScroll: true });
}


function selectNode(node, filters) {
  if (!filters) filters = { journey: details.journey };
  selectedNode = node;
  if (!filters.journey) graph.highlightTrace();
  graph.render(nodes, node.id);
  details.open(node, filters);
}


function updateTheme(theme) {
  document.documentElement.dataset.theme = theme;
  document.querySelector("#theme-label").textContent = theme === "dark" ? "Escuro" : "Claro";
  document.querySelector("#theme-button").setAttribute("aria-label", theme === "dark" ? "Usar tema claro" : "Usar tema escuro");
  saveValue("vcd-theme", theme);
}


function notice(text, error = false) {
  const node = document.querySelector("#notice");
  node.replaceChildren(text ? element("div", { className: error ? "notice error" : "notice" }, text) : document.createDocumentFragment());
}


function renderGraph() {
  nodes = buildNodes(pipelineState, overview);
  selectedNode = null;
  details.configure(nodes);
  graph.runStatus = pipelineState.execution?.status;
  graph.render(nodes);
}

function route() {
  activePage = location.hash === "#pipeline" ? "pipeline" : "map";
  const isMap = activePage === "map";
  document.querySelector("#search-form").hidden = isMap;
  const heading = document.querySelector(".page-heading");
  const refreshButton = document.querySelector("#refresh-button");
  heading.hidden = true;
  document.querySelector("main").classList.toggle("map-layout", isMap);
  document.querySelector("main").classList.toggle("pipeline-layout", !isMap);
  if (isMap) document.querySelector(".map-controls").append(refreshButton);
  else document.querySelector(".canvas-controls").append(refreshButton);
  document.querySelector("#pipeline-page").hidden = isMap;
  document.querySelector("#map-page").hidden = !isMap;
  document.querySelector("#page-title").textContent = isMap ? "Mapa do Brasil" : "Pipeline de dados";
  document.querySelector("#page-description").textContent = isMap
    ? "Um novo ponto de vista sobre a origem dos seus dados."
    : "Explore a origem, as transformações e os destinos de cada registro.";
  for (const name of ["pipeline", "map"]) {
    const tab = document.querySelector(`#${name}-tab`);
    tab.classList.toggle("active", activePage === name);
    if (activePage === name) tab.setAttribute("aria-current", "page");
    else tab.removeAttribute("aria-current");
  }
}


async function refresh() {
  clearTimeout(progressTimer);
  progressGeneration += 1;
  requestController?.abort();
  const controller = new AbortController();
  requestController = controller;
  const refreshButton = document.querySelector("#refresh-button");
  refreshButton.disabled = true;
  details.close();
  notice("");
  graph.showFeedback(message("Carregando a pipeline", "Consultando fontes, camadas e a execução atual…", null, true));
  // O mapa depende apenas dos dados publicados; não aguarda a inspeção dos arquivos da pipeline.
  const mapRequest = Promise.allSettled([
    api.get("/overview", {}, controller.signal),
    api.get("/datasets/geocoding/records", { limit: 200 }, controller.signal),
  ]).then(results => {
    if (requestController !== controller) return results;

    overview = results[0].status === "fulfilled" ? results[0].value : null;
    capitals = results[1].status === "fulfilled" ? results[1].value.items : [];
    map.configure(capitals, overview);

    if (results.some(item => item.status === "fulfilled")) {
      document.querySelector("#connection-button").classList.add("online");
    }

    return results;
  });

  const pipelineRequest = loadPipeline(api, controller.signal).then(value => {
    if (requestController === controller) {
      pipelineState = value;
      renderGraph();
    }

    return value;
  });

  const [mapResult, pipelineResult] = await Promise.all([
    mapRequest,
    pipelineRequest.then(value => ({ status: "fulfilled", value }), reason => ({ status: "rejected", reason })),
  ]);
  const results = [...mapResult, pipelineResult];
  if (requestController !== controller) return;
  refreshButton.disabled = false;
  const failures = [];
  for (const result of results) {
    if (result.status === "rejected") failures.push(result.reason.message);
  }
  const connected = results.some(item => item.status === "fulfilled");
  document.querySelector("#connection-button").classList.toggle("online", connected);
  document.querySelector("#connection-button").title = `${connected ? "Conectado" : "Sem conexão"} · ${api.base} · Configurar API`;
  if (pipelineResult.status === "rejected") {
    pipelineState = { nodes: [], execution: {} };
    renderGraph();
  }
  if (!connected) {
    graph.showFeedback(message("Não foi possível carregar", failures[0] || "Confira a conexão com o backend.", refresh));
    notice("Backend indisponível. Use o botão API para conferir o endereço.", true);
  } else if (failures.length) {
    notice("Algumas fontes não puderam ser consultadas: " + [...new Set(failures)].join(" "), true);
  } else if (!pipelineState.nodes.length && !overview?.datasets?.some(item => item.available)) {
    graph.showFeedback(message("Nenhum dado disponível", "Execute a pipeline e a carga para publicar os dados e publicar os vínculos atuais."));
    notice("Ainda não há dados publicados ou execução atual nesta conexão.");
  }
  scheduleProgress();
}


function scheduleProgress() {
  clearTimeout(progressTimer);
  const generation = progressGeneration;
  progressTimer = setTimeout(() => pollProgress(generation), 15000);
}


async function pollProgress(generation) {
  if (generation !== progressGeneration) return;
  if (document.hidden) {
    scheduleProgress();
    return;
  }

  try {
    const updated = await loadPipeline(api);
    if (generation !== progressGeneration) return;
    const changedRun = updated.execution?.run_id !== pipelineState.execution?.run_id;
    const completed = updated.execution?.status === "succeeded" && (changedRun || pipelineState.execution?.status !== "succeeded");
    if (JSON.stringify(updated) !== JSON.stringify(pipelineState)) {
      pipelineState = updated;
      nodes = buildNodes(pipelineState);
      details.nodes = nodes;
      if (changedRun) {
        selectedNode = null;
        details.configure(nodes);
        graph.highlightTrace();
      }
      graph.runStatus = pipelineState.execution?.status;
      graph.render(nodes, selectedNode?.id);
      if (completed) {
        const [nextOverview, nextCapitals] = await Promise.all([
          api.get("/overview"),
          api.get("/datasets/geocoding/records", { limit: 200 }),
        ]);
        if (generation !== progressGeneration) return;

        overview = nextOverview;
        capitals = nextCapitals.items;
        const selected = map.selected;
        const date = map.date;
        await map.configure(capitals, overview);
        if (generation !== progressGeneration) return;

        if (selected) {
          const capital = capitals.find(item => item.municipality_id === selected.municipality_id);
          if (capital) { map.date = date; map.select(capital); }
        }
      }
    }
    document.querySelector("#connection-button").classList.add("online");
  } catch (error) {
    if (generation === progressGeneration) {
      document.querySelector("#connection-button").classList.remove("online");
      document.querySelector("#connection-button").title = "Atualização pendente: " + error.message;
    }
  } finally {
    if (generation === progressGeneration) scheduleProgress();
  }
}


async function search(event, selectedQuery) {
  event.preventDefault();
  const query = selectedQuery ?? document.querySelector("#search-input").value;
  if (!query.trim()) {
    notice("");
    if (selectedNode) selectNode(selectedNode);
    return;
  }
  try {
    if (!nodes.length) throw new Error("Aguarde a conexão com a API antes de pesquisar.");
    notice("");
    suggestionVersion += 1;
    suggestionsBox.replaceChildren();
    selectedNode = null;
    graph.highlightTrace();
    graph.render(nodes);
    await details.search(query);
  } catch (error) {
    notice(error.message, true);
  }
}


const suggestionsBox = element("div", { className: "search-suggestions", "aria-label": "Municípios semelhantes" });
document.querySelector("#search-form").append(suggestionsBox);
document.querySelector("#search-input").placeholder = "Município, estação, código IBGE ou coluna=valor";
let suggestionTimer;
let suggestionVersion = 0;
document.querySelector("#search-input").addEventListener("input", () => {
  clearTimeout(suggestionTimer);
  const version = ++suggestionVersion;
  const query = document.querySelector("#search-input").value.trim();
  suggestionsBox.replaceChildren();
  if (query.length < 2 || query.includes("=")) return;
  suggestionTimer = setTimeout(async () => {
    try {
      const result = await api.get("/pipeline/suggestions", { q: query });
      if (version !== suggestionVersion) return;
      for (const item of result.items) {
        suggestionsBox.append(element("button", { type: "button", onClick: () => {
          suggestionsBox.replaceChildren();
          search({ preventDefault() {} }, "municipality_id=" + item.municipality_id);
        } }, item.name + " · " + item.state_code));
      }
    } catch {
      if (version === suggestionVersion) suggestionsBox.replaceChildren();
    }
  }, 250);
});
document.addEventListener("click", event => {
  if (!event.target.closest("#search-form")) suggestionsBox.replaceChildren();
});

document.querySelector("#search-form").addEventListener("submit", search);
document.querySelector("#refresh-button").addEventListener("click", refresh);
document.querySelector("#theme-button").addEventListener("click", () => updateTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark"));
window.addEventListener("hashchange", route);
document.addEventListener("keydown", event => {
  if (event.key === "Escape" && !document.querySelector("#connection-dialog").open) {
    if (activePage === "map") map.clear();
    else closeDetails();
  }
});

const dialog = document.querySelector("#connection-dialog");
document.querySelector("#connection-button").addEventListener("click", () => {
  document.querySelector("#api-url").value = api.base;
  document.querySelector("#connection-error").textContent = "";
  dialog.showModal();
});
document.querySelector("#cancel-connection").addEventListener("click", () => dialog.close());
document.querySelector("#connection-form").addEventListener("submit", event => {
  event.preventDefault();
  try {
    api.setBase(document.querySelector("#api-url").value);
    saveValue("vcd-api", api.base);
    dialog.close();
    refresh();
  } catch (error) {
    document.querySelector("#connection-error").textContent = error.message;
  }
});

updateTheme(storedValue("vcd-theme", "dark") === "light" ? "light" : "dark");
route();
// Exibe a malha imediatamente, antes das consultas ao backend.
map.configure([], null);
refresh();
