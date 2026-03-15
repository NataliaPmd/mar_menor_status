"""Shared utility functions for the Mar Menor scraper."""

import logging
import re
from datetime import date
from pathlib import Path
from typing import Optional

import requests

from .config import MONTHS_ES, _LAGUNA_COLS

log = logging.getLogger(__name__)


def parse_float(s: str) -> Optional[float]:
    """Parse a float from Spanish-formatted strings.

    Handles decimal commas ('1,23' → 1.23), strips units and
    whitespace, returns None if the value cannot be parsed.
    """
    if not s:
        return None
    s = s.strip()
    s = s.replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _safe_col(col: str) -> str:
    """Return col if it is in _LAGUNA_COLS, else raise ValueError.

    Called before every f-string column interpolation in SQL to prevent
    an unexpected key from producing a malformed or injected statement.
    """
    if col not in _LAGUNA_COLS:
        raise ValueError(f"Unexpected parametros_laguna column: {col!r}")
    return col


def spanish_date_to_iso(day_month: str, year_hint: int) -> Optional[str]:
    """Convert 'DD mes' (e.g. '09 mar') to 'YYYY-MM-DD'.

    If the parsed month is more than 2 months ahead of the current
    month, it is assumed to belong to the previous year.
    """
    parts = day_month.strip().lower().split()
    if len(parts) != 2:
        return None
    day_str, month_str = parts
    month_num = MONTHS_ES.get(month_str[:3])
    if not month_num:
        return None
    try:
        day = int(day_str)
    except ValueError:
        return None

    year = year_hint
    current_month = date.today().month
    if month_num > current_month + 2:
        year -= 1

    try:
        return date(year, month_num, day).isoformat()
    except ValueError:
        return None


def _download_pdf(url: str, dest: Path, tag: str) -> bool:
    """Download url to dest if dest does not already exist.

    Returns True on success (or if already cached), False on failure.
    Logs a warning and returns False on any network error so callers
    can skip the file and continue with the rest.
    """
    if dest.exists():
        return True
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return True
    except requests.RequestException as e:
        log.warning("[%s] Failed to download %s: %s", tag, url, e)
        return False
