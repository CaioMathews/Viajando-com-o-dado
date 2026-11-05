import { element, format } from "./ui.js";

export const LAYERS = ["source", "bronze", "silver", "gold", "load"];
export const LAYER_LABELS = {
  source: "Fontes",
  bronze: "Bronze",
  silver: "Silver",
  gold: "Gold",
  load: "Consumo",
};

export async function loadPipeline(api, signal) {
  return api.get("/pipeline", {}, signal);
}

export function buildNodes(state) {
  return state.nodes || [];
}

export function graphConnections(nodes) {
  const edges = [];

  for (const node of nodes) {
    for (const parentId of node.parents || []) {
      const parent = nodes.find((item) => item.id === parentId);

      if (parent) {
        edges.push({ source: parent, target: node });
      }
    }
  }

  return edges;
}

export function nodeStatus(node) {
  return (
    {
      succeeded: "Concluída",
      failed: "Falhou",
      running: "Em execução",
      waiting: "Aguardando etapa",
      available: "Dados disponíveis",
      not_started: "Não executada",
    }[node.status] || "Não executada"
  );
}

export class PipelineGraph {
  constructor(onSelect, onClear) {
    this.onSelect = onSelect;
    this.onClear = onClear;
    this.container = document.querySelector("#graph");
    this.viewport = document.querySelector("#graph-viewport");
    this.zoom = 1;
    this.position = { x: 0, y: 0 };
    document
      .querySelector("#zoom-in")
      .addEventListener("click", () => this.setZoom(this.zoom * 1.2));
    document
      .querySelector("#zoom-out")
      .addEventListener("click", () => this.setZoom(this.zoom / 1.2));
    document.querySelector("#fit-graph").addEventListener("click", () => this.fit());
    this.enableNavigation();
    this.resizeObserver = new ResizeObserver(() => {
      if (this.nodes?.length) {
        this.fit();
      }
    });
    this.resizeObserver.observe(this.viewport);
    window.addEventListener("resize", () => this.fit());
  }

  applyView() {
    if (this.nodes?.length) {
      const padding = 24;
      const width = this.viewport.clientWidth;
      const height = this.viewport.clientHeight - 60;
      const contentWidth = 1460 * this.zoom;
      const contentHeight = 940 * this.zoom;

      // O fluxo não pode ser arrastado para fora da área útil.
      this.position.x =
        contentWidth <= width - padding * 2
          ? (width - contentWidth) / 2
          : Math.max(width - padding - contentWidth, Math.min(padding, this.position.x));
      this.position.y =
        contentHeight <= height - padding * 2
          ? padding
          : Math.max(height - padding - contentHeight, Math.min(padding, this.position.y));
    }

    this.container.style.transform = `translate(${this.position.x}px, ${this.position.y}px) scale(${this.zoom})`;
    document.querySelector("#zoom-label").textContent = Math.round(this.zoom * 100) + "%";
    document.querySelector("#zoom-out").disabled = this.zoom <= 0.450001;
    document.querySelector("#zoom-in").disabled = this.zoom >= 1.249999;
  }

  setZoom(value, anchor) {
    const point = anchor || { x: this.viewport.clientWidth / 2, y: this.viewport.clientHeight / 2 };
    const next = Math.max(0.45, Math.min(1.25, value));
    const factor = next / this.zoom;

    this.position.x = point.x - (point.x - this.position.x) * factor;
    this.position.y = point.y - (point.y - this.position.y) * factor;
    this.zoom = next;
    this.applyView();
  }

  fit() {
    const width = this.viewport.clientWidth;
    const bounds = this.viewport.getBoundingClientRect?.();
    const available = bounds
      ? Math.max(420, window.innerHeight - bounds.top - 12)
      : this.viewport.clientHeight;

    if (!width || available <= 0) {
      return;
    }

    this.viewport.style.height = available + "px";
    this.zoom = Math.max(0.45, Math.min(1, (width - 48) / 1460, (available - 104) / 940));
    this.position = { x: (width - 1460 * this.zoom) / 2, y: 16 };
    this.applyView();
  }

  enableNavigation() {
    let drag = null;

    this.viewport.addEventListener("pointerdown", (event) => {
      if (event.button !== 0 || event.target.closest(".data-node")) {
        return;
      }

      drag = { x: event.clientX, y: event.clientY, position: { ...this.position }, moved: false };
    });
    this.viewport.addEventListener("pointermove", (event) => {
      if (!drag) {
        return;
      }

      const dx = event.clientX - drag.x;
      const dy = event.clientY - drag.y;

      if (!drag.moved && Math.abs(dx) + Math.abs(dy) < 5) {
        return;
      }

      drag.moved = true;
      this.viewport.setPointerCapture(event.pointerId);
      this.viewport.dataset.dragging = "true";
      this.position = { x: drag.position.x + dx, y: drag.position.y + dy };
      this.applyView();
    });
    const release = (event) => {
      if (this.viewport.hasPointerCapture(event.pointerId)) {
        this.viewport.releasePointerCapture(event.pointerId);
      }

      delete this.viewport.dataset.dragging;
      const clear = drag && !drag.moved && event.type === "pointerup";

      drag = null;

      if (clear) {
        this.onClear?.();
      }
    };

    this.viewport.addEventListener("pointerup", release);
    this.viewport.addEventListener("pointercancel", release);
    this.viewport.addEventListener(
      "wheel",
      (event) => {
        if (!this.nodes?.length) {
          return;
        }

        event.preventDefault();
        const bounds = this.viewport.getBoundingClientRect();

        this.setZoom(this.zoom * (event.deltaY < 0 ? 1.12 : 1 / 1.12), {
          x: event.clientX - bounds.left,
          y: event.clientY - bounds.top,
        });
      },
      { passive: false },
    );
    this.viewport.addEventListener("keydown", (event) => {
      if (event.target !== this.viewport) {
        return;
      }

      const movement = {
        ArrowLeft: [40, 0],
        ArrowRight: [-40, 0],
        ArrowUp: [0, 40],
        ArrowDown: [0, -40],
      }[event.key];

      if (movement) {
        event.preventDefault();
        this.position.x += movement[0];
        this.position.y += movement[1];
        this.applyView();
      } else if (["+", "="].includes(event.key)) {
        this.setZoom(this.zoom * 1.2);
      } else if (event.key === "-") {
        this.setZoom(this.zoom / 1.2);
      } else if (event.key === "Home") {
        this.fit();
      }
    });
  }

  showFeedback(content) {
    this.nodes = [];
    this.zoom = 1;
    this.position = { x: 0, y: 0 };
    this.applyView();
    this.container.replaceChildren(content);
  }

  drawConnection(svg, source, target) {
    const x1 = 30 + source.column * 288 + 220;
    const y1 = 72 + source.row * 144 + 66;
    const x2 = 30 + target.column * 288;
    const y2 = 72 + target.row * 144 + 66;
    const middle = (x1 + x2) / 2;
    const path = document.createElementNS(svg.namespaceURI, "path");

    if (target.column === source.column) {
      // Junções dentro da Gold passam pelos corredores entre os cartões.
      const right = x1 + 14 + target.row * 3;
      const left = x2 - 14 - target.row * 3;
      const lane = 72 + target.row * 144 - 6;

      path.setAttribute("d", `M ${x1} ${y1} H ${right} V ${lane} H ${left} V ${y2} H ${x2}`);
    } else if (target.column - source.column > 1) {
      const lane = 72 + target.row * 144 - 12 - source.column * 3;

      path.setAttribute("d", `M ${x1} ${y1} H ${x1 + 16} V ${lane} H ${x2 - 16} V ${y2} H ${x2}`);
    } else if (y1 === y2) {
      path.setAttribute("d", `M ${x1} ${y1} H ${x2}`);
    } else {
      path.setAttribute("d", `M ${x1} ${y1} H ${middle} V ${y2} H ${x2}`);
    }

    path.setAttribute("class", "wire " + source.layer);
    path.setAttribute("marker-end", `url(#arrow-${source.layer})`);
    path.dataset.source = source.id;
    path.dataset.target = target.id;
    path.dataset.layer = source.layer;
    svg.append(path);
  }

  highlightTrace(identifiers = [], links = []) {
    this.traceIds = new Set(identifiers);
    this.traceLinks = links;
    const connections = new Set();

    for (const link of links) {
      connections.add(link.source + "->" + link.target);
    }

    const active = this.traceIds.size > 0;

    this.container.classList.toggle("tracing", active);

    for (const card of this.container.querySelectorAll(".data-node")) {
      card.classList.toggle("on-trace", this.traceIds.has(card.dataset.node));
    }

    for (const wire of this.container.querySelectorAll(".wire")) {
      const highlighted = connections.has(wire.dataset.source + "->" + wire.dataset.target);

      wire.classList.toggle("on-trace", highlighted);
      wire.setAttribute("marker-end", `url(#arrow-${highlighted ? "trace" : wire.dataset.layer})`);
    }
  }

  render(nodes, selectedId) {
    const firstRender = !this.nodes?.length;

    this.nodes = nodes;
    this.container.replaceChildren();
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");

    svg.setAttribute("viewBox", "0 0 1460 940");
    svg.setAttribute("class", "graph-wires");
    svg.setAttribute("aria-hidden", "true");
    this.container.append(svg);
    const defs = document.createElementNS(svg.namespaceURI, "defs");

    for (const layer of [...LAYERS, "trace"]) {
      const arrow = document.createElementNS(svg.namespaceURI, "marker");

      for (const [key, value] of Object.entries({
        id: "arrow-" + layer,
        viewBox: "0 0 10 10",
        refX: "14",
        refY: "5",
        markerWidth: "8",
        markerHeight: "8",
        markerUnits: "userSpaceOnUse",
        orient: "auto",
      })) {
        arrow.setAttribute(key, value);
      }

      const shape = document.createElementNS(svg.namespaceURI, "path");

      shape.setAttribute("d", "M 0 0 L 10 5 L 0 10 Z");
      shape.setAttribute("class", "arrow " + layer);
      arrow.append(shape);
      defs.append(arrow);
    }

    svg.append(defs);

    const headings = ["Fontes", "Bronze", "Silver", "Gold", "PostgreSQL"];

    for (let index = 0; index < headings.length; index += 1) {
      this.container.append(
        element(
          "h2",
          { className: "layer-heading", style: `left:${30 + index * 288}px` },
          headings[index],
        ),
      );
    }

    const connections = graphConnections(nodes);
    const incoming = new Set();
    const outgoing = new Set();

    for (const edge of connections) {
      incoming.add(edge.target.id);
      outgoing.add(edge.source.id);
      this.drawConnection(svg, edge.source, edge.target);
    }

    for (const node of nodes) {
      const x = 30 + node.column * 288;
      const y = 72 + node.row * 144;
      const category =
        node.layer === "source"
          ? "FONTE"
          : node.layer === "load"
            ? "POSTGRESQL"
            : "TABELA · " + node.layer.toUpperCase();
      const metric = node.layer === "source" ? node.files_count : node.records;
      const metricLabel = node.layer === "source" ? "Arquivos" : "Linhas";
      const label = nodeStatus(node, this.runStatus);
      const card = element(
        "button",
        {
          className: `data-node ${node.layer} ${selectedId === node.id ? "selected" : ""} ${incoming.has(node.id) ? "has-input" : ""} ${outgoing.has(node.id) ? "has-output" : ""}`,
          style: `left:${x}px;top:${y}px`,
          "data-node": node.id,
          "aria-label": `${LAYER_LABELS[node.layer]}: ${node.title}. ${label}`,
          "aria-pressed": selectedId === node.id ? "true" : "false",
          onClick: () => this.onSelect(node),
        },
        element(
          "span",
          { className: "node-kind" },
          element(
            "span",
            { className: "node-icon", "aria-hidden": "true" },
            node.layer === "source" ? "↗" : "▤",
          ),
          category,
        ),
        element(
          "strong",
          { className: "node-title" },
          node.layer === "source" ? node.origin : node.title,
        ),
        element("span", { className: "node-status " + (node.status || "") }, label),
        element(
          "span",
          { className: "node-stats" },
          `${metricLabel}  ${format(metric)}`,
          element(
            "span",
            {},
            node.id === "gold:inmet_daily" ? "Agregação" : node.parents?.length > 1 ? "Junção" : "",
          ),
        ),
      );

      this.container.append(card);
    }

    this.highlightTrace(this.traceIds || [], this.traceLinks || []);

    if (firstRender) {
      requestAnimationFrame(() => this.fit());
    }
  }
}
