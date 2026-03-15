// ─── Biological alert thresholds ──────────────────────────────────────────────
// Change here to update charts + insights together
const CHLOROPHYLL_ALARM_UGL = 2;    // µg/L: ecological risk threshold
const CHLOROPHYLL_WARN_UGL  = 1.5;  // µg/L: early warning
const O2_CRITICAL_MGL       = 3;    // mg/L: hypoxia threshold
const O2_LOW_MGL            = 5;    // mg/L: below-optimal dissolved oxygen

// ─── Time range options for chart selectors ────────────────────────────────
const RANGES = [
  { label: "1M",  months: 1  },
  { label: "3M",  months: 3  },
  { label: "6M",  months: 6  },
  { label: "1Y",  months: 12 },
  { label: "3Y",  months: 36 },
  { label: "All", months: 0  },
];
