"""Source: AEMET daily precipitation scraper."""

import logging
import os
import sqlite3
import time
from datetime import date

import requests

from .config import AEMET_BASE, AEMET_HISTORY_START_MONTH, AEMET_HISTORY_START_YEAR, AEMET_STATION
from .utils import parse_float

log = logging.getLogger(__name__)


def scrape_aemet_precipitation(conn: sqlite3.Connection) -> dict:
    """Download daily precipitation from AEMET OpenData API.

    Station 7031 is San Javier Aeropuerto, closest station to Mar Menor.
    The API is limited to 31 days per request, so we iterate month by month
    from 2020 to today. Already-loaded months are skipped. Requires the
    AEMET_API_KEY environment variable.

    Precipitation codes:
      'Ip' = inapreciable (trace, < 0.1 mm) → stored as 0.0
      ''   = no observation for that day    → stored as NULL
    """
    import calendar

    api_key = os.environ.get("AEMET_API_KEY")
    if not api_key:
        raise RuntimeError(
            "AEMET_API_KEY environment variable not set. "
            "Get a free key at https://opendata.aemet.es"
        )

    today = date.today()
    total_inserted = 0

    history_start = date(AEMET_HISTORY_START_YEAR, AEMET_HISTORY_START_MONTH, 1)

    months = []
    cur = history_start
    while cur <= today:
        last_day_num = calendar.monthrange(cur.year, cur.month)[1]
        period_end   = min(date(cur.year, cur.month, last_day_num), today)
        months.append((cur, period_end))
        cur = date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)

    for period_start, period_end in months:
        # Skip months already fully loaded (check last day of period)
        if conn.execute(
            "SELECT 1 FROM precipitacion_aemet WHERE fecha = ? AND estacion = ?",
            (period_end.isoformat(), AEMET_STATION),
        ).fetchone():
            continue

        ini = period_start.strftime("%Y-%m-%dT00:00:00UTC")
        fin = period_end.strftime("%Y-%m-%dT23:59:59UTC")
        url = (
            f"{AEMET_BASE}/valores/climatologicos/diarios/datos"
            f"/fechaini/{ini}/fechafin/{fin}/estacion/{AEMET_STATION}"
        )
        log.info("[aemet] Requesting %s – %s", period_start, period_end)

        try:
            resp = requests.get(url, params={"api_key": api_key}, timeout=30)
            resp.raise_for_status()
            meta = resp.json()
        except (requests.RequestException, ValueError) as e:
            log.warning("[aemet] Failed to fetch %s: %s", period_start, e)
            time.sleep(2)
            continue

        if meta.get("estado") != 200:
            log.warning("[aemet] %s for %s: %s",
                        meta.get("estado"), period_start, meta.get("descripcion"))
            time.sleep(2)
            continue

        datos_url = meta.get("datos")
        try:
            records = requests.get(datos_url, timeout=30).json()
        except (requests.RequestException, ValueError) as e:
            log.warning("[aemet] Failed to fetch data for %s: %s", period_start, e)
            time.sleep(2)
            continue

        inserted = 0
        for rec in records:
            fecha    = rec.get("fecha")
            prec_raw = rec.get("prec", "")
            if not fecha:
                continue
            if prec_raw in ("", None):
                prec_mm = None
            elif str(prec_raw).strip().lower() == "ip":
                prec_mm = 0.0   # inapreciable (trace amount)
            else:
                prec_mm = parse_float(str(prec_raw))

            rows = conn.execute(
                "INSERT OR IGNORE INTO precipitacion_aemet (fecha, estacion, prec_mm) VALUES (?, ?, ?)",
                (fecha, AEMET_STATION, prec_mm),
            ).rowcount
            inserted += rows

        conn.commit()
        log.info("[aemet] %d-%02d: %d new records", period_start.year, period_start.month, inserted)
        total_inserted += inserted

        # Respect AEMET rate limit (~30 req/min = 1 req/2s)
        time.sleep(2)

    return {"source": "aemet", "new_records": total_inserted, "error": None}
