import { element } from "./ui.js";

const MONTHS = [
  "Janeiro",
  "Fevereiro",
  "Março",
  "Abril",
  "Maio",
  "Junho",
  "Julho",
  "Agosto",
  "Setembro",
  "Outubro",
  "Novembro",
  "Dezembro",
];
let pickerNumber = 0;

export function parseDate(value) {
  const text = value.trim();
  let parts = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(text);

  if (!parts) {
    parts = /^(\d{2})(\d{2})(\d{4})$/.exec(text);
  }

  if (!parts) {
    return null;
  }

  const [, day, month, year] = parts;

  if (Number(year) < 1000) {
    return null;
  }

  const parsed = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));

  if (
    parsed.getUTCFullYear() !== Number(year) ||
    parsed.getUTCMonth() !== Number(month) - 1 ||
    parsed.getUTCDate() !== Number(day)
  ) {
    return null;
  }

  return `${year}-${month}-${day}`;
}

export function formatDate(iso) {
  return iso ? iso.split("-").reverse().join("/") : "";
}

export function calendarDays(year, month) {
  const offset = (new Date(Date.UTC(year, month, 1)).getUTCDay() + 6) % 7;
  const count = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();

  return Array.from({ length: 42 }, (_, index) => {
    const day = index - offset + 1;

    return day >= 1 && day <= count
      ? `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`
      : null;
  });
}

export function datePicker({ value, minimum, maximum, onSelect }) {
  const id = "indicator-date-" + ++pickerNumber;
  const selected = value || minimum || "2025-01-01";
  let year = Number(selected.slice(0, 4));
  let month = Number(selected.slice(5, 7)) - 1;
  const firstYear = Number((minimum || selected).slice(0, 4));
  const lastYear = Number((maximum || selected).slice(0, 4));
  const allowed = (iso) => (!minimum || iso >= minimum) && (!maximum || iso <= maximum);
  const input = element("input", {
    id,
    type: "text",
    inputmode: "numeric",
    autocomplete: "off",
    placeholder: "DD/MM/AAAA",
    value: formatDate(selected),
    maxlength: "10",
    "aria-describedby": id + "-error",
  });
  const error = element("p", {
    id: id + "-error",
    className: "date-error",
    role: "alert",
    hidden: true,
  });
  const popup = element("details", { className: "date-calendar" });
  const toggle = element(
    "summary",
    { "aria-label": "Abrir calendário", title: "Escolher no calendário" },
    element("span", { className: "date-icon date-icon-calendar", "aria-hidden": "true" }),
  );
  const calendar = element("div", {
    className: "calendar-popup",
    role: "group",
    "aria-label": "Calendário dos indicadores",
  });

  popup.append(toggle, calendar);

  function showError(text) {
    error.textContent = text;
    error.hidden = !text;
    input.setAttribute("aria-invalid", String(Boolean(text)));
  }

  function choose(iso) {
    if (!allowed(iso)) {
      return;
    }

    input.value = formatDate(iso);
    showError("");
    popup.open = false;
    input.focus();

    if (iso !== value) {
      onSelect(iso);
    }
  }

  function apply() {
    const iso = parseDate(input.value);

    if (!iso) {
      showError("Informe uma data válida no formato DD/MM/AAAA.");
      input.focus();

      return;
    }

    if (!allowed(iso)) {
      showError(`Escolha entre ${formatDate(minimum)} e ${formatDate(maximum)}.`);

      return;
    }

    choose(iso);
  }

  function render() {
    const monthSelect = element("select", {
      "aria-label": "Mês",
      onChange: (event) => {
        month = Number(event.target.value);
        render();
      },
    });

    for (const [index, label] of MONTHS.entries()) {
      const start = `${year}-${String(index + 1).padStart(2, "0")}-01`;
      const end = `${year}-${String(index + 1).padStart(2, "0")}-${new Date(Date.UTC(year, index + 1, 0)).getUTCDate()}`;
      const option = element(
        "option",
        { value: index, disabled: (minimum && end < minimum) || (maximum && start > maximum) },
        label,
      );

      option.selected = index === month;
      monthSelect.append(option);
    }

    const yearSelect = element("select", {
      "aria-label": "Ano",
      onChange: (event) => {
        year = Number(event.target.value);
        render();
      },
    });

    for (let optionYear = firstYear; optionYear <= lastYear; optionYear += 1) {
      const option = element("option", { value: optionYear }, optionYear);

      option.selected = optionYear === year;
      yearSelect.append(option);
    }

    const previous = new Date(Date.UTC(year, month, 0)).toISOString().slice(0, 10);
    const next = new Date(Date.UTC(year, month + 1, 1)).toISOString().slice(0, 10);

    function move(delta) {
      const target = new Date(Date.UTC(year, month + delta, 1));

      year = target.getUTCFullYear();
      month = target.getUTCMonth();
      render();
    }

    const grid = element("div", { className: "calendar-days" });

    for (const day of ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]) {
      grid.append(element("span", { className: "calendar-weekday" }, day));
    }

    for (const iso of calendarDays(year, month)) {
      grid.append(
        iso
          ? element(
              "button",
              {
                type: "button",
                className: "calendar-day" + (iso === selected ? " selected" : ""),
                disabled: !allowed(iso),
                "aria-label": formatDate(iso),
                "aria-current": iso === selected ? "date" : null,
                onClick: () => choose(iso),
              },
              Number(iso.slice(8)),
            )
          : element("span", { "aria-hidden": "true" }),
      );
    }

    calendar.replaceChildren(
      element(
        "div",
        { className: "calendar-heading" },
        element(
          "button",
          {
            type: "button",
            "aria-label": "Mês anterior",
            disabled: !allowed(previous),
            onClick: () => move(-1),
          },
          "‹",
        ),
        monthSelect,
        yearSelect,
        element(
          "button",
          {
            type: "button",
            "aria-label": "Próximo mês",
            disabled: !allowed(next),
            onClick: () => move(1),
          },
          "›",
        ),
      ),
      grid,
      element(
        "button",
        {
          type: "button",
          className: "calendar-close",
          onClick: () => {
            popup.open = false;
            toggle.focus();
          },
        },
        "Fechar calendário",
      ),
    );
  }

  input.addEventListener("input", () => showError(""));
  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      apply();
    }
  });
  popup.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      popup.open = false;
      toggle.focus();
    }
  });
  render();

  return element(
    "div",
    { className: "indicator-date-picker" },
    element("label", { for: id }, "Data dos indicadores"),
    element(
      "div",
      { className: "date-input-row" },
      input,
      popup,
      element(
        "button",
        {
          type: "button",
          className: "date-apply",
          "aria-label": "Aplicar data",
          title: "Aplicar data",
          onClick: apply,
        },
        element("span", { className: "date-icon date-icon-check", "aria-hidden": "true" }),
      ),
    ),
    error,
  );
}
