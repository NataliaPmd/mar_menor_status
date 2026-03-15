// ─── Chart utilities ───────────────────────────────────────────────────────
// Depends on: constants.js, translations.js

// Stores full unfiltered data and Chart.js instances for range updates
const chartData = {};
const chartInstances = {};

function toNum(v) {
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}

// Returns a flat array for a dashed threshold line at y=value
function threshold(dates, value) {
  return dates.map(() => value);
}

function makeChart(id, label, dates, values, color, thresholdValue = null) {
  // Persist full data so range changes can re-slice without re-fetching
  chartData[id] = { label, dates, values, color, thresholdValue };

  // Insert range selector buttons before the canvas
  const canvas = document.getElementById(id);
  const selector = document.createElement("div");
  selector.className = "range-selector";
  selector.dataset.chart = id;
  RANGES.forEach(({ label: lbl, months }) => {
    const btn = document.createElement("button");
    btn.className = "range-btn" + (months === 0 ? " active" : "");
    btn.textContent = months === 0 ? T[lang].rangeAll : lbl;
    btn.dataset.months = months;
    btn.addEventListener("click", () => applyRange(id, months));
    selector.appendChild(btn);
  });
  canvas.parentNode.insertBefore(selector, canvas);

  // Build datasets for the initial (full) render
  const datasets = buildDatasets(dates, values, color, thresholdValue);

  chartInstances[id] = new Chart(canvas, {
    type: "line",
    data: { labels: dates, datasets },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: { mode: "index", intersect: false },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 10, maxRotation: 0 } },
        y: { beginAtZero: false },
      },
    },
  });
}

function buildDatasets(dates, values, color, thresholdValue) {
  const datasets = [{
    data: values,
    borderColor: color,
    backgroundColor: color + "22",
    borderWidth: 2,
    pointRadius: 3,
    tension: 0.3,
    fill: false,
    spanGaps: true,
  }];
  if (thresholdValue !== null) {
    datasets.push({
      label: `Threshold (${thresholdValue})`,
      data: threshold(dates, thresholdValue),
      borderColor: "#ef4444",
      borderWidth: 1.5,
      borderDash: [6, 4],
      pointRadius: 0,
      fill: false,
    });
  }
  return datasets;
}

function applyRange(id, months) {
  const d = chartData[id];

  // Mixed chart (Albujón: precipitation bars + nitrates line + chlorophyll line)
  if (d.type === 'albujon') {
    let dates, nitrates, precip, chlorophyll;
    if (months === 0) {
      dates       = d.dates;
      nitrates    = d.nitrates;
      precip      = d.precip;
      chlorophyll = d.chlorophyll;
    } else {
      const cutoff = new Date();
      cutoff.setMonth(cutoff.getMonth() - months);
      const cutoffStr = cutoff.toISOString().slice(0, 10);
      const pairs = d.dates
        .map((dt, i) => [dt, d.nitrates[i], d.precip[i], d.chlorophyll[i]])
        .filter(([dt]) => dt >= cutoffStr);
      dates       = pairs.map(p => p[0]);
      nitrates    = pairs.map(p => p[1]);
      precip      = pairs.map(p => p[2]);
      chlorophyll = pairs.map(p => p[3]);
    }
    const chart = chartInstances[id];
    chart.data.labels           = dates;
    chart.data.datasets[0].data = precip;       // bar dataset (index 0)
    chart.data.datasets[1].data = nitrates;     // line dataset (index 1)
    chart.data.datasets[2].data = chlorophyll;  // line dataset (index 2)
    chart.update();
    document.querySelectorAll(`.range-selector[data-chart="${id}"] .range-btn`)
      .forEach(btn => btn.classList.toggle("active", +btn.dataset.months === months));
    return;
  }

  const { dates, values, color, thresholdValue } = d;

  let slicedDates, slicedValues;
  if (months === 0) {
    slicedDates  = dates;
    slicedValues = values;
  } else {
    const cutoff = new Date();
    cutoff.setMonth(cutoff.getMonth() - months);
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    const pairs = dates
      .map((dt, i) => [dt, values[i]])
      .filter(([dt]) => dt >= cutoffStr);
    slicedDates  = pairs.map(p => p[0]);
    slicedValues = pairs.map(p => p[1]);
  }

  const chart = chartInstances[id];
  chart.data.labels           = slicedDates;
  chart.data.datasets[0].data = slicedValues;
  if (thresholdValue !== null) {
    chart.data.datasets[1].data = threshold(slicedDates, thresholdValue);
  }
  chart.update();

  // Highlight the active button
  document.querySelectorAll(`.range-selector[data-chart="${id}"] .range-btn`)
    .forEach(btn => btn.classList.toggle("active", +btn.dataset.months === months));
}

// ─── Albujón mixed chart: nitrates (line) + precipitation (bars) ────────────

// Sum precipitation over the 7 days ending on each aforos sample date.
// This provides the "rainfall context" for each nitrate measurement.
function buildWeeklyPrecip(afDates, precipByDate) {
  return afDates.map(fecha => {
    let sum = 0, hasData = false;
    for (let i = 0; i < 7; i++) {
      const d = new Date(fecha + 'T12:00:00Z');
      d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      const v = precipByDate[key];
      if (v != null) { sum += v; hasData = true; }
    }
    return hasData ? sum : null;
  });
}

function makeAlbujonChart(id, dates, nitrates, precip, chlorophyll) {
  chartData[id] = { type: 'albujon', dates, nitrates, precip, chlorophyll };

  const canvas = document.getElementById(id);
  const selector = document.createElement("div");
  selector.className = "range-selector";
  selector.dataset.chart = id;
  RANGES.forEach(({ label: lbl, months }) => {
    const btn = document.createElement("button");
    btn.className = "range-btn" + (months === 0 ? " active" : "");
    btn.textContent = months === 0 ? T[lang].rangeAll : lbl;
    btn.dataset.months = months;
    btn.addEventListener("click", () => applyRange(id, months));
    selector.appendChild(btn);
  });
  canvas.parentNode.insertBefore(selector, canvas);

  chartInstances[id] = new Chart(canvas, {
    data: {
      labels: dates,
      datasets: [
        {
          type: 'bar',
          label: '7-day rainfall (mm)',
          data: precip,
          backgroundColor: '#94a3b833',
          borderColor: '#94a3b855',
          borderWidth: 1,
          yAxisID: 'yPrecip',
          order: 3,
        },
        {
          type: 'line',
          label: 'Nitrates (mg/L)',
          data: nitrates,
          borderColor: '#f97316',
          backgroundColor: '#f9731622',
          borderWidth: 2,
          pointRadius: 3,
          tension: 0.3,
          fill: false,
          spanGaps: true,
          yAxisID: 'yNitrates',
          order: 1,
        },
        {
          type: 'line',
          label: 'Chlorophyll (µg/L)',
          data: chlorophyll,
          borderColor: '#22c55e',
          backgroundColor: '#22c55e22',
          borderWidth: 2,
          pointRadius: 2,
          tension: 0.3,
          fill: false,
          spanGaps: true,
          yAxisID: 'yNitrates',
          order: 2,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
        tooltip: { mode: 'index', intersect: false },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 10, maxRotation: 0 } },
        yNitrates: {
          type: 'linear',
          position: 'left',
          beginAtZero: false,
          title: { display: true, text: 'Nitrates (mg/L) · Chlorophyll (µg/L)', font: { size: 11 } },
        },
        yPrecip: {
          type: 'linear',
          position: 'right',
          beginAtZero: true,
          grid: { drawOnChartArea: false },
          title: { display: true, text: 'Rainfall 7d (mm)', font: { size: 11 } },
        },
      },
    },
  });
}
