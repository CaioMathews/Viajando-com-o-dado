import { format } from "./ui.js";


export const WEATHER_METRICS = [
  { key: "temperature_mean_c", label: "Temperatura média", unit: "°C", prefix: "temperature", rule: "Média das temperaturas horárias válidas. No INMET, exige pelo menos 90% das horas esperadas do dia." },
  { key: "temperature_min_c", label: "Temperatura mínima", unit: "°C", prefix: "temperature_min", rule: "Menor temperatura do dia. No INMET, exige todas as horas esperadas para publicar o extremo diário." },
  { key: "temperature_max_c", label: "Temperatura máxima", unit: "°C", prefix: "temperature_max", rule: "Maior temperatura do dia. No INMET, exige todas as horas esperadas para publicar o extremo diário." },
  { key: "precipitation_sum_mm", label: "Chuva", unit: "mm", prefix: "precipitation", rule: "Soma da precipitação do dia, em milímetros. No INMET, exige todas as horas esperadas; zero significa ausência de chuva medida." },
  { key: "wind_speed_max_ms", label: "Vento máximo", unit: "m/s", prefix: "wind", rule: "Maior velocidade do vento no dia, em metros por segundo. No INMET, exige todas as horas esperadas." },
  { key: "wind_gust_max_ms", label: "Rajada máxima", unit: "m/s", prefix: "gust", rule: "Maior rajada de vento registrada no dia, em metros por segundo. Publicada pelo INMET quando todas as horas esperadas têm medição válida." },
];


export function weatherMetrics(row, source) {
  const metrics = [];

  for (const metric of WEATHER_METRICS) {
    if (source === "open_meteo" && metric.key === "wind_gust_max_ms" && !Object.hasOwn(row, metric.key)) continue;
    metrics.push(metric);
  }

  return metrics;
}


export function metricAbsence(row, metric, source) {
  if (row[metric.key] !== null && row[metric.key] !== undefined) return "";
  if (row.assignment_status === "no_eligible_station") return "Sem estação elegível.";

  if (source === "inmet") {
    return `${format(row[metric.prefix + "_hours"])} de ${format(row.expected_hours)} horas válidas. Cobertura insuficiente para publicar o indicador.`;
  }

  return "Indicador ausente ou invalidado na fonte; consulte a aba Qualidade.";
}


export function qualityFacts(row, source) {
  if (source === "population") {
    return [
      ["Estimativa", row.population_estimate == null ? "Ausente" : "Disponível"],
      ["Referência", format(row.reference_date)],
      ["Nota da fonte", row.population_note || "Sem nota adicional"],
    ];
  }

  if (source === "open_meteo") {
    let available = 0;
    const metrics = weatherMetrics(row, source);

    for (const metric of metrics) {
      if (row[metric.key + "_available"] === true) available += 1;
    }

    const labels = { complete: "Completo", partial: "Parcial", missing: "Ausente" };
    const facts = [
      ["Registro na fonte", row.source_record_present === true ? "Presente" : row.source_record_present === false ? "Ausente" : "Não informado"],
      ["Situação", labels[row.record_status] || row.record_status || "Não informada"],
      ["Indicadores disponíveis", `${available} de ${metrics.length}`],
    ];

    for (const [key, label] of [["missing_fields", "Campos ausentes"], ["invalid_fields", "Campos inválidos"], ["quality_flags", "Alertas"]]) {
      const values = row[key];
      facts.push([label, Array.isArray(values) ? values.length ? values.join(" · ") : "Nenhum" : "Não informado"]);
    }

    return facts;
  }

  const assignments = { assigned: "Estação associada", no_eligible_station: "Sem estação elegível" };
  const boundaries = { compatible: "Compatíveis", missing: "Ausentes", incompatible: "Incompatíveis" };
  return [
    ["Estação", assignments[row.assignment_status] || row.assignment_status || "Não informada"],
    ["Horas esperadas no dia", format(row.expected_hours)],
    ["Dados de borda", boundaries[row.boundary_metadata_status] || row.boundary_metadata_status || "Não informados"],
  ];
}
