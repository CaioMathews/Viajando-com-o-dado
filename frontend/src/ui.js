export const number = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 2 });

export function element(tag, attributes = {}, ...children) {
  const node = document.createElement(tag);

  for (const [key, value] of Object.entries(attributes)) {
    if (value === undefined || value === null || value === false) {
      continue;
    }

    if (key === "className") {
      node.className = value;
    } else if (key.startsWith("on")) {
      node.addEventListener(key.slice(2).toLowerCase(), value);
    } else if (key === "checked" || key === "disabled" || key === "hidden" || key === "open") {
      node[key] = value;
    } else {
      node.setAttribute(key, value);
    }
  }

  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) {
      continue;
    }

    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }

  return node;
}

export function format(value, unit = "") {
  if (value === undefined || value === null) {
    return "—";
  }

  if (value === "") {
    return '""';
  }

  if (typeof value === "number") {
    return number.format(value) + (unit ? ` ${unit}` : "");
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}

export function formatCell(value, column) {
  const identifier = column === "id" || column.endsWith("_id") || column.endsWith(".id");

  if (identifier && (typeof value === "number" || typeof value === "bigint")) {
    return String(value);
  }

  return format(value);
}

export function dateTime(value) {
  if (!value) {
    return "—";
  }

  const date = new Date(value);

  return Number.isNaN(date.getTime()) ? value : date.toLocaleString("pt-BR");
}

export function message(title, description, retry, loading = false) {
  const box = element(
    "div",
    { className: "feedback", role: loading ? "status" : "region" },
    element(
      "span",
      { className: loading ? "feedback-icon spinner" : "feedback-icon", "aria-hidden": "true" },
      loading ? "" : "!",
    ),
    element("div", {}, element("h3", {}, title), element("p", {}, description)),
  );

  if (retry) {
    box.append(
      element("button", { className: "button primary", onClick: retry }, "Tentar novamente"),
    );
  }

  return box;
}

export function jsonBlock(value) {
  return element("pre", { className: "json-block", tabindex: "0" }, JSON.stringify(value, null, 2));
}

export function table(rows, action, preferred = []) {
  if (!rows.length) {
    return element("p", { className: "empty" }, "Nenhum registro encontrado para estes filtros.");
  }

  const columns = new Set(preferred);

  for (const row of rows) {
    for (const key of Object.keys(row)) {
      columns.add(key);
    }
  }

  const head = element("tr");

  if (action) {
    head.append(element("th", { scope: "col" }, "Caminho"));
  }

  for (const key of columns) {
    head.append(element("th", { scope: "col" }, key));
  }

  const body = element("tbody");

  for (const row of rows) {
    const tr = element("tr");

    if (action) {
      tr.append(
        element(
          "td",
          {},
          element("button", { className: "text-button", onClick: () => action(row) }, "Rastrear →"),
        ),
      );
    }

    for (const key of columns) {
      const label = formatCell(row[key], key);

      tr.append(
        element(
          "td",
          { className: row[key] === null ? "null-value" : "", title: label },
          row[key] === null ? "NULL" : label,
        ),
      );
    }

    body.append(tr);
  }

  return element(
    "div",
    {
      className: "table-scroll",
      tabindex: "0",
      "aria-label": "Tabela de registros com rolagem horizontal",
    },
    element("table", {}, element("thead", {}, head), body),
  );
}

export function tabbedContent(definitions, initialKey) {
  const navigation = element("div", {
    className: "capital-tabs detail-tabs",
    role: "tablist",
    "aria-label": "Detalhes da etapa",
  });
  const content = element("div", { className: "detail-tab-content" });
  const wrapper = element("div", { className: "detail-tab-layout" }, navigation, content);
  const buttons = [];
  const panels = new Map();
  const loaded = new Set();

  async function select(key) {
    for (const button of buttons) {
      const selected = button.getAttribute("data-tab") === key;

      button.setAttribute("aria-selected", String(selected));
      button.setAttribute("tabindex", selected ? "0" : "-1");
    }

    for (const [name, panel] of panels) {
      panel.hidden = name !== key;
    }

    if (loaded.has(key)) {
      return;
    }

    loaded.add(key);
    const panel = panels.get(key);

    panel.replaceChildren(message("Carregando", "Consultando os dados da etapa…", null, true));

    try {
      const definition = definitions.find((item) => item.key === key);

      panel.replaceChildren(await definition.load());
    } catch (error) {
      loaded.delete(key);
      panel.replaceChildren(message("Não foi possível carregar", error.message, () => select(key)));
    }
  }

  for (const definition of definitions) {
    const button = element(
      "button",
      {
        role: "tab",
        id: "detail-tab-" + definition.key,
        "data-tab": definition.key,
        "aria-controls": "detail-section-" + definition.key,
        onClick: () => select(definition.key),
      },
      definition.label,
    );
    const panel = element("div", {
      role: "tabpanel",
      id: "detail-section-" + definition.key,
      "aria-labelledby": "detail-tab-" + definition.key,
    });

    buttons.push(button);
    panels.set(definition.key, panel);
    navigation.append(button);
    content.append(panel);
  }

  navigation.addEventListener("keydown", (event) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
      return;
    }

    event.preventDefault();
    const current = buttons.indexOf(document.activeElement);
    let next = (current + (event.key === "ArrowLeft" ? -1 : 1) + buttons.length) % buttons.length;

    if (event.key === "Home") {
      next = 0;
    }

    if (event.key === "End") {
      next = buttons.length - 1;
    }

    select(buttons[next].getAttribute("data-tab"));
    buttons[next].focus();
  });

  select(initialKey || definitions[0].key);

  return wrapper;
}

let tooltipNumber = 0;

export function infoLabel(label, description) {
  const id = "metric-help-" + ++tooltipNumber;
  const help = element(
    "span",
    { className: "metric-help" },
    element(
      "button",
      {
        className: "info-button",
        type: "button",
        "aria-label": "Sobre " + label,
        "aria-describedby": id,
        title: description,
      },
      "i",
    ),
    element("span", { id, role: "tooltip", className: "metric-tooltip" }, description),
  );

  return element("span", { className: "metric-label" }, help, label);
}
