-- Mar Menor ecological monitoring database schema
-- All dates stored as ISO 8601 strings (YYYY-MM-DD) for easy sorting and JS parsing

-- Water quality parameters from the lagoon
-- Sources: 'html' (monitoring page), 'cdg' (CdG weekly PDFs), 'imida' (IMIDA weekly PDFs)
CREATE TABLE IF NOT EXISTS parametros_laguna (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha           TEXT NOT NULL,          -- YYYY-MM-DD
    fuente          TEXT NOT NULL,          -- 'html' | 'cdg' | 'imida'
    temperatura     REAL,                   -- °C
    salinidad       REAL,                   -- PSU
    clorofila_ug_l  REAL,                   -- µg/L (chlorophyll)
    oxigeno_mg_l    REAL,                   -- mg/L (dissolved oxygen)
    turbidez_ftu    REAL,                   -- FTU (turbidity)
    transparencia_m REAL,                   -- m (water transparency)
    UNIQUE(fecha, fuente)
);

-- Flow and nutrient data from Rambla del Albujón
CREATE TABLE IF NOT EXISTS aforos_albujon (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha           TEXT NOT NULL UNIQUE,   -- YYYY-MM-DD
    caudal_l_s      REAL,                   -- l/s (flow rate)
    nitratos_mg_l   REAL                    -- mg/L (nitrates)
);

-- Download audit log: one row per source per run
CREATE TABLE IF NOT EXISTS meta_downloads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,          -- 'html' | 'cdg' | 'imida' | 'aforos'
    download_date   TEXT NOT NULL,          -- YYYY-MM-DD of the run
    new_records     INTEGER,                -- number of rows inserted this run
    error           TEXT,                   -- NULL if successful, error message otherwise
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);
