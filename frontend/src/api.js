import { API_URL } from "../config.js";


export function storedValue(key, fallback) {
  try {
    return localStorage.getItem(key) || fallback;
  } catch {
    return fallback;
  }
}


export function saveValue(key, value) {
  try {
    localStorage.setItem(key, value);
  } catch {
    // A sessão continua funcionando quando o armazenamento do navegador está bloqueado.
  }
}


export class Api {
  constructor(base = storedValue("vcd-api", API_URL)) {
    this.setBase(base);
  }

  setBase(value) {
    const url = new URL(value);

    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) {
      throw new Error("Informe um endereço HTTP ou HTTPS sem credenciais.");
    }

    this.base = url.href.replace(/\/$/, "");
  }

  url(path, parameters = {}) {
    const url = new URL(this.base + path);

    for (const [key, value] of Object.entries(parameters)) {
      if (value !== undefined && value !== null && value !== "") {
        url.searchParams.set(key, value);
      }
    }

    return url;
  }

  async get(path, parameters = {}, signal) {
    // Consultas de observações podem ler arquivos maiores da camada atual.
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 180000);
    const abort = () => controller.abort();
    signal?.addEventListener("abort", abort, { once: true });

    if (signal?.aborted) controller.abort();

    try {
      const response = await fetch(this.url(path, parameters), { signal: controller.signal });
      let body;
      try {
        body = await response.json();
      } catch {
        throw new Error(`A API está temporariamente indisponível (HTTP ${response.status}). Aguarde a inicialização e tente novamente.`);
      }

      if (!response.ok) {
        let message = body.detail;

        if (Array.isArray(message)) {
          const messages = [];

          for (const item of message) messages.push(item.msg);
          message = messages.join("; ");
        }

        const error = new Error(message || `A API respondeu com HTTP ${response.status}.`);
        error.status = response.status;
        throw error;
      }

      return body;
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error("A consulta foi cancelada ou excedeu o tempo de espera. Tente novamente.");
      }

      if (error instanceof TypeError) {
        throw new Error("Não foi possível conectar à API. Confira se o backend está iniciado e se o endereço está correto.");
      }

      throw error;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
    }
  }

}
