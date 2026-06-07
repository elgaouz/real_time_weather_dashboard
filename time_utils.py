"""Morocco wall-clock (IANA: Africa/Casablanca) for producer + dashboard."""
from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

MOROCCO_TZ = ZoneInfo(os.getenv("DISPLAY_TZ", "Africa/Casablanca"))


def now_morocco_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(MOROCCO_TZ).strftime(fmt)
