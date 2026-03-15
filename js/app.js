// ─── Main application: data loading, chart rendering, language switching ───
// Depends on: constants.js, translations.js, charts.js, insights.js

// ─── Data store ────────────────────────────────────────────────────────────
const store = { params: null, aforos: null, precip: null };

function tryRender() {
  if (store.params !== null && store.aforos !== null && store.precip !== null) renderCharts();
}

// ─── Load CSVs ─────────────────────────────────────────────────────────────
Papa.parse("datos/parametros.csv", {
  download: true,
  header: true,
  skipEmptyLines: true,
  complete: ({ data }) => { store.params = data; tryRender(); },
  error: (err) => {
    document.getElementById("last-updated").textContent = T[lang].loadError;
    console.error("parametros.csv:", err);
  },
});

Papa.parse("datos/aforos.csv", {
  download: true,
  header: true,
  skipEmptyLines: true,
  complete: ({ data }) => { store.aforos = data; tryRender(); },
  error: (err) => {
    console.error("aforos.csv:", err);
    store.aforos = [];
    tryRender();
  },
});

Papa.parse("datos/precipitacion.csv", {
  download: true,
  header: true,
  skipEmptyLines: true,
  complete: ({ data }) => { store.precip = data; tryRender(); },
  error: () => { store.precip = []; tryRender(); },  // graceful fallback if not yet generated
});

// ─── Deduplicate lagoon records ─────────────────────────────────────────────
// Priority: cdg (3) > imida (2) > html (1) > upct (0).
// This ensures that when multiple sources have data for the same date,
// only the best one is shown, while html fills in dates not covered by
// cdg/imida (mainly the most recent weeks of the current year).
function dedup(rows) {
  const PRIORITY = { cdg: 3, imida: 2, html: 1, upct: 0 };
  const best = {};
  for (const r of rows) {
    const p = PRIORITY[r.fuente] || 0;
    if (!best[r.fecha] || p > (PRIORITY[best[r.fecha].fuente] || 0)) {
      best[r.fecha] = r;
    }
  }
  return Object.values(best).sort((a, b) => a.fecha.localeCompare(b.fecha));
}

// ─── Render all charts ──────────────────────────────────────────────────────
function renderCharts() {
  const params  = store.params;
  const aforos  = store.aforos;

  // Merge all sources, keeping one record per date (cdg > imida > html)
  const lagoon = dedup(params);
  const lagoonDates = lagoon.map(r => r.fecha);

  makeChart(
    "chart-chlorophyll",
    "Chlorophyll (µg/L)",
    lagoonDates,
    lagoon.map(r => toNum(r.clorofila_ug_l)),
    "#22c55e",
    CHLOROPHYLL_ALARM_UGL,
  );

  makeChart(
    "chart-oxygen",
    "Dissolved Oxygen (mg/L)",
    lagoonDates,
    lagoon.map(r => toNum(r.oxigeno_mg_l)),
    "#3b82f6",
    O2_CRITICAL_MGL,
  );

  makeChart(
    "chart-transparency",
    "Transparency (m)",
    lagoonDates,
    lagoon.map(r => toNum(r.transparencia_m)),
    "#06b6d4",
  );

  makeChart(
    "chart-temperature",
    "Water Temperature (°C)",
    lagoonDates,
    lagoon.map(r => toNum(r.temperatura)),
    "#f97316",
  );

  makeChart(
    "chart-salinity",
    "Salinity (PSU)",
    lagoonDates,
    lagoon.map(r => toNum(r.salinidad)),
    "#8b5cf6",
  );

  makeChart(
    "chart-turbidity",
    "Turbidity (FTU)",
    lagoonDates,
    lagoon.map(r => toNum(r.turbidez_ftu)),
    "#d97706",
  );

  // Build precipitation lookup (date → mm) for the Albujón mixed chart
  const precipByDate = {};
  for (const r of store.precip) {
    const v = toNum(r.prec_mm);
    if (v !== null) precipByDate[r.fecha] = v;
  }

  // Build chlorophyll lookup (date → µg/L) from deduplicated lagoon data
  const chlByDate = {};
  for (const r of lagoon) {
    const v = toNum(r.clorofila_ug_l);
    if (v !== null) chlByDate[r.fecha] = v;
  }

  const nitMap = Object.fromEntries(aforos.map(r => [r.fecha, toNum(r.nitratos_mg_l)]));

  // Find the first date that has a nitrates reading to avoid a long empty lead-in
  const firstNitDate = aforos
    .filter(r => toNum(r.nitratos_mg_l) != null)
    .map(r => r.fecha)
    .sort()[0];

  // Merge aforos and lagoon dates into a single sorted axis, starting from first nitrates date
  const mergedDates = [...new Set([
    ...aforos.map(r => r.fecha),
    ...Object.keys(chlByDate),
  ])].sort().filter(d => !firstNitDate || d >= firstNitDate);

  const mergedNitrates = mergedDates.map(d => nitMap[d]    ?? null);
  const mergedChl      = mergedDates.map(d => chlByDate[d] ?? null);
  const mergedPrecip   = buildWeeklyPrecip(mergedDates, precipByDate);

  makeAlbujonChart("chart-nitratos", mergedDates, mergedNitrates, mergedPrecip, mergedChl);

  // Phosphates chart — same date axis as nitrates (aforos sampling dates)
  const fosfDates  = aforos.filter(r => toNum(r.fosfatos_mg_l) != null).map(r => r.fecha).sort();
  const fosfValues = fosfDates.map(d => {
    const r = aforos.find(a => a.fecha === d);
    return r ? toNum(r.fosfatos_mg_l) : null;
  });
  makeChart("chart-fosfatos", "Orthophosphates (mg/L)", fosfDates, fosfValues, "#ec4899");

  // Last updated: most recent date across both datasets
  const allDates = [
    ...lagoonDates,
    ...aforos.map(r => r.fecha),
  ].filter(Boolean).sort();

  const lastDate = allDates[allDates.length - 1] ?? "unknown";
  document.getElementById("last-updated").textContent = T[lang].lastUpdated(lastDate);

  renderInsights(lagoon, aforos);
}

// ─── Language switching ─────────────────────────────────────────────────────
function setLang(l) {
  lang = l;
  document.documentElement.lang = l;
  document.title = l === 'es' ? 'Estado del Mar Menor' : 'Mar Menor Status';

  // Update all static data-i18n elements
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.dataset.i18n;
    const val = T[l][key];
    if (val !== undefined) el.innerHTML = val;
  });

  // Update range "All/Todo" buttons (created dynamically by makeChart)
  document.querySelectorAll('.range-btn[data-months="0"]').forEach(btn => {
    btn.textContent = T[l].rangeAll;
  });

  // Highlight active lang button
  document.querySelectorAll('.lang-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.lang === l);
  });

  // Re-render dynamic content if data is loaded
  if (store.params && store.aforos && store.precip !== null) {
    const lagoon = dedup(store.params);
    const allDates = [...lagoon.map(r => r.fecha), ...store.aforos.map(r => r.fecha)]
      .filter(Boolean).sort();
    const lastDate = allDates[allDates.length - 1] ?? 'unknown';
    document.getElementById('last-updated').textContent = T[l].lastUpdated(lastDate);
    renderInsights(lagoon, store.aforos);
  }
}
