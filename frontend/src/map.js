import { element, format, message, infoLabel } from "./ui.js";
import { WEATHER_METRICS, weatherMetrics, qualityFacts, metricAbsence } from "./metrics.js";


const SVG_NS = "http://www.w3.org/2000/svg";
const INITIAL_VIEW = [0, 0, 800, 780];


function project(longitude, latitude) {
  return [(longitude + 75) * 18, (6 - latitude) * 18];
}


function geometryPath(geometry) {
  const polygons = geometry.type === "MultiPolygon" ? geometry.coordinates : [geometry.coordinates];
  let path = "";

  for (const polygon of polygons) {
    for (const ring of polygon) {
      let first = true;
      for (const point of ring) {
        const [x, y] = project(point[0], point[1]);
        path += `${first ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
        first = false;
      }
      path += "Z";
    }
  }

  return path;
}


export class BrazilMap {
  constructor(api, onAudit) {
    this.api = api;
    this.onAudit = onAudit;
    this.svg = document.querySelector("#brazil-map");
    this.panel = document.querySelector("#capital-panel");
    this.request = 0;
    this.view = [...INITIAL_VIEW];
    this.date = "";
    document.querySelector("#fit-map").addEventListener("click", () => this.clear());
    document.querySelector("#map-zoom-in").addEventListener("click", () => this.zoom(0.75));
    document.querySelector("#map-zoom-out").addEventListener("click", () => this.zoom(1.3));
    this.enablePan();
    this.svg.addEventListener("click", event => {
      if (this.ignoreClick) return;
      const stateOrCapital = event.target.closest(".state-shape, .capital-marker");
      if (!stateOrCapital) this.clear();
    });
  }

  enablePan() {
    let origin = null;
    let moved = false;
    this.svg.addEventListener("pointerdown", event => {
      if (event.button !== 0) return;
      origin = { x: event.clientX, y: event.clientY, view: [...this.view] };
      moved = false;
    });
    this.svg.addEventListener("pointermove", event => {
      if (!origin) return;
      const dx = event.clientX - origin.x;
      const dy = event.clientY - origin.y;
      if (Math.abs(dx) + Math.abs(dy) < 5) return;
      moved = true;
      this.svg.setPointerCapture(event.pointerId);
      const rect = this.svg.getBoundingClientRect();
      const scale = Math.max(origin.view[2] / rect.width, origin.view[3] / rect.height);
      this.setView([origin.view[0] - dx * scale, origin.view[1] - dy * scale, origin.view[2], origin.view[3]]);
    });
    this.svg.addEventListener("pointerup", event => {
      if (this.svg.hasPointerCapture(event.pointerId)) this.svg.releasePointerCapture(event.pointerId);
      origin = null;
      if (moved) {
        this.ignoreClick = true;
        setTimeout(() => { this.ignoreClick = false; }, 0);
      }
    });
    this.svg.addEventListener("pointercancel", () => { origin = null; });
  }

  setView(view) {
    this.view = view;
    this.svg.setAttribute("viewBox", view.join(" "));
    document.querySelector("#map-zoom-label").textContent = Math.round(INITIAL_VIEW[2] / view[2] * 100) + "%";
    const bounds = this.svg.getBoundingClientRect();
    if (!bounds.width || !bounds.height) return;

    // Mantém os pontos legíveis sem ampliá-los junto com o estado.
    const scale = Math.max(view[2] / bounds.width, view[3] / bounds.height);
    for (const circle of this.svg.querySelectorAll(".capital-marker circle")) {
      circle.setAttribute("r", (circle.classList.contains("marker-target") ? 11 : 4) * scale);
    }
  }

  zoom(factor) {
    const width = Math.max(100, Math.min(1600, this.view[2] * factor));
    const height = width * INITIAL_VIEW[3] / INITIAL_VIEW[2];
    this.setView([this.view[0] + (this.view[2] - width) / 2, this.view[1] + (this.view[3] - height) / 2, width, height]);
  }

  async configure(capitals, overview) {
    const configuration = (this.configuration || 0) + 1;
    this.configuration = configuration;
    this.request += 1;
    this.capitals = capitals;
    this.overview = overview;
    const weather = overview?.datasets?.find(item => item.name === "open_meteo_daily");
    this.minimumDate = weather?.period_start;
    this.maximumDate = weather?.period_end;
    this.date = this.minimumDate || "2025-01-01";
    this.svg.replaceChildren();
    try {
      // A geometria é estática: compartilha a mesma leitura entre configurações.
      if (!this.geography) {
        this.geography = fetch("./assets/brazil-states.geojson").then(response => {
          if (!response.ok) throw new Error("A malha do mapa não pôde ser carregada.");
          return response.json();
        }).catch(error => {
          this.geography = null;
          throw error;
        });
      }

      const geography = await this.geography;
      if (configuration !== this.configuration) return;
      for (const feature of geography.features) {
        const path = document.createElementNS(SVG_NS, "path");
        path.setAttribute("d", geometryPath(feature.geometry));
        path.setAttribute("class", "state-shape");
        path.dataset.state = feature.properties.codarea;
        const capital = capitals.find(item => String(item.municipality_id).slice(0, 2) === String(feature.properties.codarea));
        if (capital) {
          path.setAttribute("role", "button");
          path.setAttribute("tabindex", "0");
          path.setAttribute("aria-label", `Selecionar ${capital.name}, ${capital.state_code}`);
          this.activate(path, () => this.select(capital));
        }
        this.svg.append(path);
      }
      for (const capital of capitals) {
        if (typeof capital.longitude !== "number" || typeof capital.latitude !== "number") continue;
        const [x, y] = project(capital.longitude, capital.latitude);
        const group = document.createElementNS(SVG_NS, "g");
        group.setAttribute("class", "capital-marker");
        group.setAttribute("role", "button");
        group.setAttribute("tabindex", "0");
        group.setAttribute("aria-label", capital.name + " · " + capital.state_code);
        group.dataset.capital = capital.municipality_id;
        group.setAttribute("transform", `translate(${x},${y})`);
        const target = document.createElementNS(SVG_NS, "circle");
        target.setAttribute("r", "11");
        target.setAttribute("class", "marker-target");
        const point = document.createElementNS(SVG_NS, "circle");
        point.setAttribute("r", "4");
        const title = document.createElementNS(SVG_NS, "title");
        title.textContent = capital.name + " · " + capital.state_code;
        group.append(title, target, point);
        this.activate(group, () => this.select(capital));
        this.svg.append(group);
      }
      document.querySelector("#map-message").replaceChildren();
      this.clear();
    } catch (error) {
      if (configuration !== this.configuration) return;
      document.querySelector("#map-message").replaceChildren(message("Mapa indisponível", error.message, () => this.configure(capitals, overview)));
      this.emptyPanel();
    }
  }

  activate(target, action) {
    target.addEventListener("click", () => { if (!this.ignoreClick) action(); });
    target.addEventListener("keydown", event => {
      if (["Enter", " "].includes(event.key)) { event.preventDefault(); action(); }
    });
  }

  clear() {
    this.request += 1;
    this.selected = null;
    this.setView([...INITIAL_VIEW]);
    this.highlight();
    this.emptyPanel();
  }

  highlight() {
    for (const path of this.svg.querySelectorAll(".state-shape")) {
      path.classList.toggle("selected", path.dataset.state === String(this.selected?.municipality_id).slice(0, 2));
    }
    this.svg.querySelector(".state-outline")?.remove();
    const selectedShape = this.svg.querySelector(".state-shape.selected");

    if (selectedShape) {
      const outline = document.createElementNS(SVG_NS, "g");
      outline.setAttribute("class", "state-outline");
      outline.setAttribute("aria-hidden", "true");

      for (const color of ["dark", "light"]) {
        const line = document.createElementNS(SVG_NS, "path");
        line.setAttribute("d", selectedShape.getAttribute("d"));
        line.setAttribute("class", "state-outline-" + color);
        outline.append(line);
      }

      this.svg.insertBefore(outline, this.svg.querySelector(".capital-marker"));
    }

    for (const point of this.svg.querySelectorAll(".capital-marker")) {
      point.classList.toggle("selected", point.dataset.capital === String(this.selected?.municipality_id));
      point.setAttribute("aria-pressed", point.dataset.capital === String(this.selected?.municipality_id) ? "true" : "false");
    }
  }

  capitalSelect() {
    const select = element("select", { "aria-label": "Selecionar capital" }, element("option", { value: "" }, "Escolha uma capital"));
    const capitals = [...this.capitals].sort((a, b) => a.name.localeCompare(b.name, "pt-BR"));
    for (const capital of capitals) select.append(element("option", { value: capital.municipality_id }, capital.name + " · " + capital.state_code));
    select.value = this.selected?.municipality_id || "";
    select.addEventListener("change", () => {
      const capital = this.capitals.find(item => String(item.municipality_id) === select.value);
      if (capital) this.select(capital);
      else this.clear();
    });
    return select;
  }

  emptyPanel() {
    this.panel.hidden = true;
    this.panel.closest(".map-workspace").classList.remove("has-capital");
    this.panel.replaceChildren(element("span", { className: "eyebrow" }, "EXPLORAÇÃO GEOGRÁFICA"),
      element("h2", {}, "O Brasil em dados"), element("p", {}, "Selecione uma capital no mapa ou na lista para consultar os indicadores publicados."),
      this.capitalSelect(), element("div", { className: "map-summary" }, element("strong", {}, format(this.capitals.length)), element("span", {}, "capitais disponíveis")),
      element("p", { className: "info-note" }, "As informações ausentes são preservadas. Uma capital sem estação INMET elegível continua visível no mapa."));
  }

  focusState(capital) {
    const code = String(capital.municipality_id).slice(0, 2);
    const shape = this.svg.querySelector(`[data-state="${code}"]`);

    if (!shape) return;

    const bounds = shape.getBBox();
    const viewport = this.svg.getBoundingClientRect();
    const ratio = viewport.width / viewport.height || INITIAL_VIEW[2] / INITIAL_VIEW[3];
    let width = Math.max(bounds.width * 1.7, 200);
    let height = Math.max(bounds.height * 1.7, 200);

    // Enquadra o estado inteiro e mantém uma margem nas quatro direções.
    if (width / height < ratio) width = height * ratio;
    else height = width / ratio;

    this.setView([
      bounds.x + bounds.width / 2 - width / 2,
      bounds.y + bounds.height / 2 - height / 2,
      width,
      height,
    ]);
  }

  async select(capital, focus = true) {
    const ticket = ++this.request;
    this.selected = capital;
    this.panel.hidden = false;
    this.panel.closest(".map-workspace").classList.add("has-capital");
    this.highlight();
    if (focus) this.focusState(capital);

    const dateInput = element("input", { type: "date", value: this.date, min: this.minimumDate, max: this.maximumDate, "aria-label": "Data das observações meteorológicas" });
    dateInput.addEventListener("change", () => {
      if (!dateInput.value || !dateInput.reportValidity()) return;
      this.date = dateInput.value;
      this.select(capital, false);
    });

    const tabs = element("div", { className: "capital-tabs", role: "tablist", "aria-label": "Informações da capital" });
    const sections = element("div", { className: "capital-tab-content" });
    const buttons = [];
    const panels = new Map();
    const definitions = [["summary", "Resumo"], ["weather", "Open-Meteo"], ["inmet", "INMET"], ["quality", "Qualidade"]];
    const choose = key => {
      this.activeTab = key;
      for (const button of buttons) {
        const selected = button.getAttribute("data-tab") === key;
        button.setAttribute("aria-selected", String(selected));
        button.setAttribute("tabindex", selected ? "0" : "-1");
      }
      for (const [name, panel] of panels) {
        panel.hidden = name !== key;
      }
    };

    for (const [key, label] of definitions) {
      const button = element("button", { role: "tab", id: "capital-tab-" + key, "data-tab": key,
        "aria-controls": "capital-section-" + key, onClick: () => choose(key) }, label);
      const panel = element("div", { role: "tabpanel", id: "capital-section-" + key,
        "aria-labelledby": "capital-tab-" + key });
      panel.append(message("Carregando indicadores", "Consultando as fontes publicadas…", null, true));
      panels.set(key, panel);
      buttons.push(button);
      tabs.append(button);
      sections.append(panel);
    }

    tabs.addEventListener("keydown", event => {
      if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      const current = buttons.indexOf(document.activeElement);
      let next = event.key === "ArrowLeft" ? (current + 3) % 4 : (current + 1) % 4;
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = 3;
      choose(buttons[next].getAttribute("data-tab"));
      buttons[next].focus();
    });
    choose(this.activeTab || "summary");

    const exported = { capital, observation_date: this.date, population: null, open_meteo: null, inmet: null };
    const download = this.downloadButton(exported, `${capital.state_code}-${this.date}.json`);
    download.disabled = true;

    this.panel.replaceChildren(
      element("button", { className: "close-button capital-close", "aria-label": "Fechar capital", onClick: () => this.clear() }, "×"),
      element("div", { className: "capital-card-heading" },
        element("div", {}, element("span", { className: "eyebrow" }, "CAPITAL SELECIONADA"), element("h2", {}, capital.name)),
        element("span", { className: "badge" }, capital.state_code + " · " + capital.state_name)),
      element("div", { className: "capital-controls" },
        element("label", {}, "Capital", this.capitalSelect()), element("label", {}, "Data dos indicadores", dateInput)),
      tabs, sections,
      element("div", { className: "capital-card-actions" },
        element("button", { className: "button primary", onClick: () => this.onAudit(capital) }, "Explorar na pipeline →"),
        download));

    const parameters = { municipality_id: capital.municipality_id, limit: 1 };
    const results = await Promise.allSettled([
      this.api.get("/datasets/population/records", parameters),
      this.api.get("/datasets/open_meteo_daily/records", { ...parameters, start_date: this.date, end_date: this.date }),
      this.api.get("/datasets/inmet_daily/records", { ...parameters, start_date: this.date, end_date: this.date }),
    ]);
    if (ticket !== this.request) return;

    const summary = panels.get("summary");
    summary.replaceChildren(element("h3", {}, "Identificação geográfica"),
      element("dl", { className: "key-values" }, element("dt", {}, infoLabel("Código IBGE", "Identificador oficial do município no IBGE. Permite relacionar a capital entre as fontes.")), element("dd", {}, capital.municipality_id),
        element("dt", {}, infoLabel("Latitude", "Coordenada da capital publicada na Gold, em graus decimais. Valores negativos ficam ao sul do Equador.")), element("dd", {}, format(capital.latitude, "°")),
        element("dt", {}, infoLabel("Longitude", "Coordenada da capital publicada na Gold, em graus decimais. Valores negativos ficam a oeste de Greenwich.")), element("dd", {}, format(capital.longitude, "°")),
        element("dt", {}, infoLabel("Região", "Grande região brasileira à qual o município pertence, segundo o cadastro do IBGE.")), element("dd", {}, capital.region_name)));
    const quality = panels.get("quality");
    const qualitySource = element("select", { "aria-label": "Fonte da qualidade" });
    for (const [value, label] of [["population", "IBGE · População"], ["open_meteo", "Open-Meteo"], ["inmet", "INMET"]]) {
      qualitySource.append(element("option", { value }, label));
    }
    qualitySource.value = this.qualitySource || "inmet";
    qualitySource.addEventListener("change", () => {
      this.qualitySource = qualitySource.value;
      for (const section of quality.querySelectorAll("[data-quality-source]")) {
        section.hidden = section.dataset.qualitySource !== qualitySource.value;
      }
    });
    quality.replaceChildren(element("h3", {}, "Qualidade dos dados"), qualitySource);
    const labels = ["População · IBGE", "Open-Meteo", "INMET"];

    for (let index = 0; index < results.length; index += 1) {
      const result = results[index];
      const target = index === 0 ? summary : panels.get(index === 1 ? "weather" : "inmet");
      if (index > 0) target.replaceChildren(element("h3", {}, labels[index]));
      if (result.status === "rejected") {
        target.append(message("Fonte indisponível", result.reason.message, () => this.select(capital, false)));
        quality.append(element("p", { className: "error-text", "data-quality-source": ["population", "open_meteo", "inmet"][index], hidden: qualitySource.value !== ["population", "open_meteo", "inmet"][index] }, labels[index] + ": fonte indisponível."));
        continue;
      }
      const row = result.value.items[0];
      if (!row) {
        target.append(element("p", { className: "empty" }, "Nenhum registro disponível para a seleção."));
        quality.append(element("p", { "data-quality-source": ["population", "open_meteo", "inmet"][index], hidden: qualitySource.value !== ["population", "open_meteo", "inmet"][index] }, labels[index] + ": sem registro para a seleção."));
        continue;
      }
      const source = ["population", "open_meteo", "inmet"][index];
      exported[source] = row;
      download.disabled = false;

      if (index === 0) {
        target.append(element("h3", {}, infoLabel(labels[index], "Estimativa da população residente do município divulgada pelo IBGE, na data de referência indicada.")),
          element("div", { className: "population-value" }, format(row.population_estimate),
            element("small", {}, "habitantes · referência " + format(row.reference_date))));
      } else {
        if (row.assignment_status === "no_eligible_station") target.append(element("p", { className: "info-note" }, "Sem estação INMET elegível pelos critérios de distância e cobertura do período."));
        const grid = element("div", { className: "weather-grid" });
        for (const metric of weatherMetrics(row, source)) {
          const description = source === "open_meteo"
            ? `${metric.label} diário fornecido pelo Open-Meteo, em ${metric.unit}. Valores ausentes ou inválidos são preservados como indisponíveis.`
            : metric.rule;
          const box = this.metric(metric.label, row[metric.key], metric.unit, description);
          const absence = metricAbsence(row, metric, source);
          if (absence) box.append(element("small", { className: "metric-absence" }, absence));
          grid.append(box);
        }
        target.append(grid);
        if (source === "open_meteo" && !Object.hasOwn(row, "wind_gust_max_ms")) {
          target.append(element("p", { className: "info-note" }, "Rajadas: esta coleta do Open-Meteo não inclui essa variável. Consulte os dados do INMET."));
        }
        if (row.station_name) target.append(element("p", { className: "info-note" },
          infoLabel(`${row.station_name} · ${format(row.station_distance_km, "km")}`,
            `Distância em linha reta entre a capital e a estação associada. A seleção considera o limite de ${format(row.max_station_distance_km, "km")} e a cobertura mínima de ${format(row.minimum_station_coverage == null ? null : row.minimum_station_coverage * 100, "%")} de temperatura e chuva no período.`)));
        if (index === 2) {
          target.append(element("div", { className: "coverage-row" },
            this.metric("Cobertura de temperatura", row.temperature_coverage == null ? null : row.temperature_coverage * 100, "%", "Percentual das horas esperadas no dia com temperatura válida. A média exige pelo menos 90%."),
            this.metric("Cobertura de chuva", row.precipitation_coverage == null ? null : row.precipitation_coverage * 100, "%", "Percentual das horas esperadas no dia com chuva válida. A soma diária exige 100%.")));
        }
      }
      const facts = element("dl", { className: "key-values quality-facts" });
      for (const [label, value] of qualityFacts(row, source)) {
        facts.append(element("div", { className: "quality-fact" }, element("dt", {}, label), element("dd", {}, value)));
      }
      const qualitySection = element("section", { className: "quality-summary", "data-quality-source": source, hidden: qualitySource.value !== source }, element("strong", {}, labels[index]), facts);
      if (source === "inmet" && row.assignment_status !== "no_eligible_station") {
        const body = element("tbody");
        for (const metric of WEATHER_METRICS) {
          body.append(element("tr", {}, element("td", {}, infoLabel(metric.label, metric.rule)),
            element("td", {}, `${format(row[metric.prefix + "_hours"])} / ${format(row.expected_hours)}`),
            element("td", {}, format(row[metric.prefix + "_coverage"] == null ? null : row[metric.prefix + "_coverage"] * 100, "%"))));
        }
        qualitySection.append(element("table", { className: "coverage-table" },
          element("thead", {}, element("tr", {}, element("th", {}, "Indicador"), element("th", {}, "Horas válidas"), element("th", {}, "Cobertura"))), body));
      }
      quality.append(qualitySection);
    }
  }

  downloadButton(record, filename) {
    return element("button", { className: "button record-download", onClick: () => {
      const url = URL.createObjectURL(new Blob([JSON.stringify(record, null, 2)], { type: "application/json" }));
      const link = element("a", { href: url, download: filename });
      link.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } }, "↓ Baixar JSON");
  }

  metric(label, value, unit, description = "Indicador publicado pela fonte para a seleção atual.") {
    return element("div", { className: "metric" }, infoLabel(label, description), element("strong", {}, format(value, unit)));
  }
}
