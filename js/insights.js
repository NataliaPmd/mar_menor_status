// ─── Insights panel ────────────────────────────────────────────────────────
// Depends on: constants.js, translations.js, charts.js (toNum, fmt)

function corr(xs, ys) {
  const n = xs.length;
  if (n < 3) return null;
  const mx = xs.reduce((a, b) => a + b, 0) / n;
  const my = ys.reduce((a, b) => a + b, 0) / n;
  const num = xs.reduce((s, x, i) => s + (x - mx) * (ys[i] - my), 0);
  const dx  = Math.sqrt(xs.reduce((s, x) => s + (x - mx) ** 2, 0));
  const dy  = Math.sqrt(ys.reduce((s, y) => s + (y - my) ** 2, 0));
  return (dx && dy) ? num / (dx * dy) : null;
}

function fmt(n, decimals = 2) {
  return n == null ? "—" : n.toFixed(decimals);
}

function renderInsights(lagoon, aforos) {
  // ── Collect paired values for correlations ──────────────────────────────
  const chlTransPairs = lagoon.filter(r => toNum(r.clorofila_ug_l) != null && toNum(r.transparencia_m) != null);
  const tempO2Pairs   = lagoon.filter(r => toNum(r.temperatura) != null && toNum(r.oxigeno_mg_l) != null);

  const rChlTrans = corr(
    chlTransPairs.map(r => toNum(r.clorofila_ug_l)),
    chlTransPairs.map(r => toNum(r.transparencia_m)),
  );
  const rTempO2 = corr(
    tempO2Pairs.map(r => toNum(r.temperatura)),
    tempO2Pairs.map(r => toNum(r.oxigeno_mg_l)),
  );

  // ── Worst chlorophyll event ─────────────────────────────────────────────
  const chlRows = lagoon.filter(r => toNum(r.clorofila_ug_l) != null);
  const worstChl = chlRows.reduce((a, b) =>
    toNum(b.clorofila_ug_l) > toNum(a.clorofila_ug_l) ? b : a, chlRows[0]);
  const worstChlVal  = toNum(worstChl?.clorofila_ug_l);
  const worstChlDate = worstChl?.fecha ?? "";

  // Min O2 on record
  const o2Rows  = lagoon.filter(r => toNum(r.oxigeno_mg_l) != null);
  const minO2Row = o2Rows.reduce((a, b) =>
    toNum(b.oxigeno_mg_l) < toNum(a.oxigeno_mg_l) ? b : a, o2Rows[0]);
  const minO2Val  = toNum(minO2Row?.oxigeno_mg_l);
  const minO2Date = minO2Row?.fecha ?? "";

  // ── Recovery: worst year vs best recent year ────────────────────────────
  const byYear = {};
  for (const r of chlRows) {
    const y = r.fecha.slice(0, 4);
    if (!byYear[y]) byYear[y] = [];
    byYear[y].push(toNum(r.clorofila_ug_l));
  }
  const yearAvgs = Object.entries(byYear)
    .map(([y, vals]) => ({ year: y, avg: vals.reduce((a, b) => a + b, 0) / vals.length }))
    .sort((a, b) => a.year.localeCompare(b.year));

  const worstYear = yearAvgs.reduce((a, b) => b.avg > a.avg ? b : a, yearAvgs[0]);
  // Best recent year = lowest avg among years with at least 6 data points
  const recentBest = yearAvgs
    .filter(e => byYear[e.year].length >= 6)
    .reduce((a, b) => b.avg < a.avg ? b : a);
  const recoveryPct = worstYear && recentBest
    ? Math.round((1 - recentBest.avg / worstYear.avg) * 100) : null;

  // Transparency: earliest year avg vs latest year with data
  const byYearTrans = {};
  for (const r of lagoon) {
    const v = toNum(r.transparencia_m);
    if (v == null) continue;
    const y = r.fecha.slice(0, 4);
    if (!byYearTrans[y]) byYearTrans[y] = [];
    byYearTrans[y].push(v);
  }
  const transYears = Object.entries(byYearTrans)
    .map(([y, vals]) => ({ year: y, avg: vals.reduce((a, b) => a + b, 0) / vals.length }))
    .sort((a, b) => a.year.localeCompare(b.year));
  const firstTransYear = transYears[0];
  const lastTransYear  = transYears[transYears.length - 1];

  // ── Seasonal chlorophyll ────────────────────────────────────────────────
  const byMonth = Array.from({ length: 12 }, () => []);
  for (const r of chlRows) {
    const m = parseInt(r.fecha.slice(5, 7), 10) - 1;
    byMonth[m].push(toNum(r.clorofila_ug_l));
  }
  const monthAvg = byMonth.map(vs => vs.length ? vs.reduce((a, b) => a + b, 0) / vs.length : null);
  // Worst month index (0-based)
  const worstMonthIdx = monthAvg.reduce((best, v, i) => (v != null && (best === -1 || v > monthAvg[best])) ? i : best, -1);
  const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  // Autumn avg (Sep=8, Oct=9, Nov=10) vs summer avg (Jun=5, Jul=6)
  const autumnAvg = [8,9,10].map(i => monthAvg[i]).filter(v => v != null);
  const summerAvg = [5,6].map(i => monthAvg[i]).filter(v => v != null);
  const autumnMean = autumnAvg.reduce((a,b)=>a+b,0)/autumnAvg.length;
  const summerMean = summerAvg.reduce((a,b)=>a+b,0)/summerAvg.length;

  // ── Max nitrate ─────────────────────────────────────────────────────────
  const nitRows = aforos.filter(r => toNum(r.nitratos_mg_l) != null);
  const maxNitRow = nitRows.reduce((a, b) =>
    toNum(b.nitratos_mg_l) > toNum(a.nitratos_mg_l) ? b : a, nitRows[0]);
  const maxNitVal  = toNum(maxNitRow?.nitratos_mg_l);
  const maxNitDate = maxNitRow?.fecha ?? "";
  const avgNit = nitRows.reduce((s, r) => s + toNum(r.nitratos_mg_l), 0) / nitRows.length;
  const nitMultiple = maxNitVal && avgNit ? Math.round(maxNitVal / avgNit) : null;

  // ── Current status ──────────────────────────────────────────────────────
  const lastRow = [...lagoon].reverse().find(r => r.fecha);
  const curChl   = toNum(lastRow?.clorofila_ug_l);
  const curO2    = toNum(lastRow?.oxigeno_mg_l);
  const curTrans = toNum(lastRow?.transparencia_m);
  const curDate  = lastRow?.fecha ?? "";

  // ── Status banner ───────────────────────────────────────────────────────
  let bannerColor = "#f0fdf4"; let dotColor = "#22c55e"; let borderColor = "#bbf7d0"; let textColor = "#166534";
  if (curChl != null && curChl >= CHLOROPHYLL_ALARM_UGL) {
    bannerColor = "#fef2f2"; dotColor = "#ef4444"; borderColor = "#fecaca"; textColor = "#991b1b";
  } else if (curChl != null && curChl >= CHLOROPHYLL_WARN_UGL) {
    bannerColor = "#fffbeb"; dotColor = "#f59e0b"; borderColor = "#fde68a"; textColor = "#92400e";
  }

  const t = T[lang];
  const chlStatus = curChl == null ? "—"
    : curChl >= CHLOROPHYLL_ALARM_UGL ? t.statusChlAbove(fmt(curChl))
    : curChl >= CHLOROPHYLL_WARN_UGL  ? t.statusChlNear(fmt(curChl))
    : t.statusChlHealthy(fmt(curChl));
  const o2Status  = curO2  == null ? "" : (curO2 < O2_CRITICAL_MGL ? t.statusO2Critical(fmt(curO2)) : curO2 < O2_LOW_MGL ? t.statusO2Low(fmt(curO2)) : t.statusO2Healthy(fmt(curO2))) + " ";
  const trStatus  = curTrans == null ? "" : t.statusTrans(fmt(curTrans)) + " ";

  document.getElementById("status-banner").innerHTML = `
    <div class="status-banner" style="background:${bannerColor};border-color:${borderColor};color:${textColor}">
      <span class="dot" style="background:${dotColor}"></span>
      <span><strong>${t.statusLabel} (${curDate}):</strong>
      ${chlStatus}. ${o2Status}${trStatus}
      ${t.statusWinter}</span>
    </div>`;

  // ── Cards ───────────────────────────────────────────────────────────────
  function card(type, title, value, unit, body) {
    return `<div class="insight-card ${type}">
      <h3>${title}</h3>
      <div class="value">${value} <small>${unit}</small></div>
      <p>${body}</p>
    </div>`;
  }

  const timesAbove = worstChlVal ? Math.round(worstChlVal / CHLOROPHYLL_ALARM_UGL) : "?";
  const worstYear4 = worstChl?.fecha?.slice(0, 4) ?? "";

  const cards = [
    card("alert",
      t.worstTitle(worstYear4),
      fmt(worstChlVal, 1), "µg/L",
      t.worstBody(timesAbove, worstChlDate, fmt(minO2Val), minO2Date)
    ),
    card("good",
      t.recovTitle(worstYear.year),
      recoveryPct != null ? `−${recoveryPct} %` : "—", `vs. ${worstYear.year}`,
      t.recovBody(
        worstYear.year, fmt(worstYear.avg),
        recentBest?.year, fmt(recentBest?.avg),
        firstTransYear?.year, firstTransYear ? fmt(firstTransYear.avg, 1) : null,
        lastTransYear?.year,  lastTransYear  ? fmt(lastTransYear.avg, 1)  : null,
      )
    ),
    card("info",
      t.chlTransTitle,
      rChlTrans != null ? `r = ${fmt(rChlTrans, 2)}` : "—", "chl / transp.",
      t.chlTransBody(
        rChlTrans != null ? fmt(Math.abs(rChlTrans), 2) : "—",
        curTrans != null ? fmt(curTrans, 1) : null
      )
    ),
    card("info",
      t.heatO2Title,
      rTempO2 != null ? `r = ${fmt(rTempO2, 2)}` : "—", "temp / O₂",
      t.heatO2Body(fmt(Math.abs(rTempO2 ?? 0), 2), fmt(minO2Val), minO2Date)
    ),
    card("info",
      t.autumnTitle,
      MONTH_NAMES[worstMonthIdx], "↑ chl",
      t.autumnBody(MONTH_NAMES[worstMonthIdx], fmt(autumnMean, 1), fmt(summerMean, 1))
    ),
    card("warning",
      t.nitratesTitle(maxNitDate),
      fmt(maxNitVal, 2), "mg/L",
      t.nitratesBody(nitMultiple != null ? nitMultiple + "×" : "—", fmt(avgNit, 2))
    ),
  ];

  document.getElementById("insight-grid").innerHTML = cards.join("\n");
}
