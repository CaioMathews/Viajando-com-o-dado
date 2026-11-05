import assert from "node:assert/strict";
import { test } from "node:test";
import { calendarDays, formatDate, parseDate } from "../src/date-picker.js";

test("a digitação aceita a data completa e números sem separadores", () => {
  assert.equal(parseDate("17/03/2025"), "2025-03-17");
  assert.equal(parseDate("17032025"), "2025-03-17");
  assert.equal(formatDate("2025-03-17"), "17/03/2025");
});

test("datas incompletas, inexistentes e anos bissextos são validados", () => {
  for (const value of ["", "17/03/202", "31/04/2025", "29/02/2025", "00/03/2025", "12/13/2025"]) {
    assert.equal(parseDate(value), null, value);
  }

  assert.equal(parseDate("29/02/2024"), "2024-02-29");
});

test("o calendário começa na segunda-feira e conserva os dias do mês", () => {
  const march = calendarDays(2025, 2);

  assert.equal(march.length, 42);
  assert.equal(march[5], "2025-03-01");
  assert.equal(march.filter(Boolean).length, 31);
  assert.equal(new Set(march.filter(Boolean)).size, 31);
  assert.equal(calendarDays(2025, 1).filter(Boolean).length, 28);
  assert.equal(calendarDays(2024, 1).filter(Boolean).length, 29);
  assert.equal(calendarDays(2025, 11).filter(Boolean).at(-1), "2025-12-31");
});

test("editar a data não dispara consultas antes da confirmação e respeita o período", async () => {
  const { datePicker } = await import("../src/date-picker.js");
  const originalDocument = globalThis.document;
  const originalNode = globalThis.Node;

  class TestNode {
    constructor(tag, text = "") {
      this.tag = tag;
      this.textContent = text;
      this.children = [];
      this.attributes = {};
      this.listeners = {};
    }
    setAttribute(key, value) {
      this.attributes[key] = String(value);

      if (key === "value") {
        this.value = String(value);
      }
    }
    append(...children) {
      this.children.push(...children);
    }
    replaceChildren(...children) {
      this.children = children;
    }
    addEventListener(name, listener) {
      this.listeners[name] = listener;
    }
    focus() {}
  }

  globalThis.Node = TestNode;
  globalThis.document = {
    createElement: (tag) => new TestNode(tag),
    createTextNode: (text) => new TestNode("#text", text),
  };

  try {
    const calls = [];
    const picker = datePicker({
      value: "2025-01-01",
      minimum: "2025-01-01",
      maximum: "2025-12-31",
      onSelect: (value) => calls.push(value),
    });
    const input = picker.children[1].children[0];
    const error = picker.children.find((child) => child.attributes.id?.endsWith("-error"));

    input.value = "17/03/202";
    input.listeners.input();
    assert.deepEqual(calls, []);
    input.listeners.keydown({ key: "Enter", preventDefault() {} });
    assert.equal(error.hidden, false);
    assert.deepEqual(calls, []);
    input.value = "17/03/2025";
    input.listeners.keydown({ key: "Enter", preventDefault() {} });
    assert.deepEqual(calls, ["2025-03-17"]);
    assert.equal(error.hidden, true);
    input.value = "17/03/2026";
    picker.children[1].children[2].listeners.click();
    assert.equal(error.hidden, false);
    assert.deepEqual(calls, ["2025-03-17"]);
  } finally {
    if (originalDocument === undefined) {
      delete globalThis.document;
    } else {
      globalThis.document = originalDocument;
    }

    if (originalNode === undefined) {
      delete globalThis.Node;
    } else {
      globalThis.Node = originalNode;
    }
  }
});
