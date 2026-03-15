"""Shared paths and constants for the Mar Menor scraper."""

from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "marmenor.db"
PDF_DIR  = BASE_DIR / "pdfs"
DATA_DIR = BASE_DIR / "datos"
SCHEMA   = BASE_DIR / "schema.sql"

PDF_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# ─── Spanish month abbreviation map ───────────────────────────────────────────

MONTHS_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4,
    "may": 5, "jun": 6, "jul": 7, "ago": 8,
    "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

# Parameter keywords used to identify rows in PDFs
PARAM_KEYWORDS = {
    "temperatura":    "temperatura",
    "salinidad":      "salinidad",
    "clorofila":      "clorofila_ug_l",
    "oxígeno":        "oxigeno_mg_l",
    "oxigeno":        "oxigeno_mg_l",
    "turbidez":       "turbidez_ftu",
    "transparencia":  "transparencia_m",
}

# Allowed column names for parametros_laguna — used to validate f-string SQL
# so that an unexpected dict key can never produce a malformed SQL statement.
_LAGUNA_COLS = frozenset({
    "temperatura", "salinidad", "clorofila_ug_l",
    "oxigeno_mg_l", "turbidez_ftu", "transparencia_m",
})

# ─── UPCT ─────────────────────────────────────────────────────────────────────

# Maps UPCT variable names to (DB column, negate) pairs.
# Transparencia values are stored as negative depths; we take abs().
UPCT_VARS = {
    "Transparencia": ("transparencia_m",  True),
    "Clorofila":     ("clorofila_ug_l",   False),
    "Oxigeno":       ("oxigeno_mg_l",     False),
    "Temperatura":   ("temperatura",      False),
    "Salinidad":     ("salinidad",        False),
    "Turbidez":      ("turbidez_ftu",     False),
}
UPCT_BASE = "https://marmenor.upct.es/thredds/fileServer/L4/{var}.csv"

# ─── AEMET ────────────────────────────────────────────────────────────────────

AEMET_BASE          = "https://opendata.aemet.es/opendata/api"
AEMET_STATION       = "7031"          # San Javier Aeropuerto, closest to Mar Menor
AEMET_HISTORY_START_YEAR  = 2020
AEMET_HISTORY_START_MONTH = 1
