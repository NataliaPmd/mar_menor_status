"""Database helpers: connection, schema init, seeding, export, and run logging."""

import csv
import logging
import sqlite3
from datetime import date
from typing import Optional

from .config import DATA_DIR, DB_PATH, SCHEMA

log = logging.getLogger(__name__)


def get_db() -> sqlite3.Connection:
    """Open SQLite connection and initialize schema if needed."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    with open(SCHEMA) as f:
        conn.executescript(f.read())
    # Column migrations for existing databases (SQLite has no IF NOT EXISTS for ALTER TABLE)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(aforos_albujon)").fetchall()}
    if "fosfatos_mg_l" not in existing:
        conn.execute("ALTER TABLE aforos_albujon ADD COLUMN fosfatos_mg_l REAL")
    conn.commit()
    return conn


def log_result(conn: sqlite3.Connection, source: str, new_records: int, error: Optional[str]):
    """Write one row to meta_downloads for the current run."""
    conn.execute(
        "INSERT INTO meta_downloads (source, download_date, new_records, error) VALUES (?, ?, ?, ?)",
        (source, date.today().isoformat(), new_records, error),
    )
    conn.commit()


def seed_from_csv(conn: sqlite3.Connection):
    """Load existing CSV data into SQLite at startup.

    This makes scraper.py idempotent across stateless CI runs:
    the CSVs in the repo are the canonical record of what has
    already been processed.
    """
    params_csv = DATA_DIR / "parametros.csv"
    aforos_csv = DATA_DIR / "aforos.csv"

    if params_csv.exists():
        with open(params_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                conn.execute(
                    """INSERT OR IGNORE INTO parametros_laguna
                       (fecha, fuente, temperatura, salinidad, clorofila_ug_l,
                        oxigeno_mg_l, turbidez_ftu, transparencia_m)
                       VALUES (:fecha, :fuente, :temperatura, :salinidad,
                               :clorofila_ug_l, :oxigeno_mg_l,
                               :turbidez_ftu, :transparencia_m)""",
                    row,
                )
        conn.commit()
        log.info("Seeded parametros_laguna from %s", params_csv)

    if aforos_csv.exists():
        with open(aforos_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                conn.execute(
                    """INSERT OR IGNORE INTO aforos_albujon
                       (fecha, caudal_l_s, nitratos_mg_l, fosfatos_mg_l)
                       VALUES (:fecha, :caudal_l_s, :nitratos_mg_l,
                               :fosfatos_mg_l)""",
                    {**row, "fosfatos_mg_l": row.get("fosfatos_mg_l") or None},
                )
        conn.commit()
        log.info("Seeded aforos_albujon from %s", aforos_csv)

    precip_csv = DATA_DIR / "precipitacion.csv"
    if precip_csv.exists():
        with open(precip_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                conn.execute(
                    """INSERT OR IGNORE INTO precipitacion_aemet
                       (fecha, estacion, prec_mm)
                       VALUES (:fecha, :estacion, :prec_mm)""",
                    row,
                )
        conn.commit()
        log.info("Seeded precipitacion_aemet from %s", precip_csv)


def export_csv(conn: sqlite3.Connection):
    """Write parametros.csv, aforos.csv, and precipitacion.csv to datos/.

    These static files are committed to the repo and read by index.html
    via PapaParse — no backend needed.
    """
    params_path = DATA_DIR / "parametros.csv"
    aforos_path = DATA_DIR / "aforos.csv"

    rows = conn.execute(
        """SELECT fecha, fuente, temperatura, salinidad, clorofila_ug_l,
                  oxigeno_mg_l, turbidez_ftu, transparencia_m
           FROM parametros_laguna
           ORDER BY fecha ASC"""
    ).fetchall()
    with open(params_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "fuente", "temperatura", "salinidad",
                         "clorofila_ug_l", "oxigeno_mg_l", "turbidez_ftu", "transparencia_m"])
        for row in rows:
            writer.writerow([row[k] if row[k] is not None else "" for k in row.keys()])
    log.info("Exported %d rows to %s", len(rows), params_path)

    rows = conn.execute(
        "SELECT fecha, caudal_l_s, nitratos_mg_l, fosfatos_mg_l FROM aforos_albujon ORDER BY fecha ASC"
    ).fetchall()
    with open(aforos_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "caudal_l_s", "nitratos_mg_l", "fosfatos_mg_l"])
        for row in rows:
            writer.writerow([row[k] if row[k] is not None else "" for k in row.keys()])
    log.info("Exported %d rows to %s", len(rows), aforos_path)

    precip_path = DATA_DIR / "precipitacion.csv"
    rows = conn.execute(
        "SELECT fecha, estacion, prec_mm FROM precipitacion_aemet ORDER BY fecha ASC"
    ).fetchall()
    with open(precip_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "estacion", "prec_mm"])
        for row in rows:
            writer.writerow([row[k] if row[k] is not None else "" for k in row.keys()])
    log.info("Exported %d rows to %s", len(rows), precip_path)
