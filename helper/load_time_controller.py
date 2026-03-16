"""
Central Load Time Controller

Purpose:
- Provide consistent load_date timestamps for HUB, LINK, and SAT layers.
- Support two modes:
    1) BATCH     -> single ingestion window (seconds offsets)
    2) TIMELINE  -> spread across BUSINESS_START_DATE to AS_OF_DATE

Important:
- This module is non-breaking.
- Builders can start using it gradually.
- If not used, existing logic remains unchanged.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Tuple
import random


class LoadTimeController:
    def __init__(
        self,
        load_mode: str = "BATCH",
        run_base_timestamp: Optional[datetime] = None,
        hub_offset_range: Tuple[int, int] = (0, 30),
        link_offset_range: Tuple[int, int] = (30, 120),
        sat_offset_range: Tuple[int, int] = (120, 600),
        timeline_start_date: Optional[str] = None,
        timeline_end_date: Optional[str] = None,
        seed: Optional[int] = None,   # <-- NEW (optional, non-breaking)
    ):
        self.load_mode = (load_mode or "BATCH").upper()

        # Use a local RNG so timestamps are deterministic and isolated from global random.
        self.rng = random.Random(seed) if seed is not None else random.Random()

        # Base timestamp anchor (used in BATCH mode)
        self.base_ts = run_base_timestamp or datetime.now()

        self.hub_offset_range = hub_offset_range
        self.link_offset_range = link_offset_range
        self.sat_offset_range = sat_offset_range

        # Timeline mode window
        self.timeline_start = (
            datetime.fromisoformat(timeline_start_date)
            if timeline_start_date
            else None
        )

        # timeline_end_date may be None (AS_OF_DATE None). Treat as "now".
        if timeline_end_date:
            self.timeline_end = datetime.fromisoformat(timeline_end_date)
        else:
            self.timeline_end = datetime.now()

    # =====================================================
    # Internal helpers
    # =====================================================

    def _random_offset(self, offset_range: Tuple[int, int]) -> timedelta:
        lo, hi = int(offset_range[0]), int(offset_range[1])
        if hi < lo:
            hi = lo
        seconds = self.rng.randint(lo, hi)
        return timedelta(seconds=seconds)

    def _random_timeline_date(self) -> datetime:
        if not self.timeline_start:
            return datetime.now()

        # inclusive-ish spread across [start, end]
        delta_days = max((self.timeline_end - self.timeline_start).days, 0)
        random_days = self.rng.randint(0, max(delta_days, 1))
        random_seconds = self.rng.randint(0, 86399)
        return self.timeline_start + timedelta(days=random_days, seconds=random_seconds)

    # =====================================================
    # Public API
    # =====================================================

    def get_hub_load_ts(self) -> str:
        """HUB load timestamp."""
        if self.load_mode == "TIMELINE":
            ts = self._random_timeline_date()
        else:
            ts = self.base_ts + self._random_offset(self.hub_offset_range)
        return ts.isoformat()

    def get_link_load_ts(self, parent_ts: Optional[str] = None) -> str:
        """
        LINK load timestamp.
        Ensures link >= parent timestamp (if provided) in BATCH mode.
        """
        if self.load_mode == "TIMELINE":
            ts = self._random_timeline_date()
        else:
            base = datetime.fromisoformat(parent_ts) if parent_ts else self.base_ts
            ts = base + self._random_offset(self.link_offset_range)
        return ts.isoformat()

    def get_sat_load_ts(self, parent_ts: Optional[str] = None) -> str:
        """
        SAT load timestamp.
        Ensures sat >= parent timestamp (if provided) in BATCH mode.
        """
        if self.load_mode == "TIMELINE":
            ts = self._random_timeline_date()
        else:
            base = datetime.fromisoformat(parent_ts) if parent_ts else self.base_ts
            ts = base + self._random_offset(self.sat_offset_range)
        return ts.isoformat()
