from __future__ import annotations

from misc.raw_to_silver_sample import build_silver


def build_api_silver(raw_dir, out_dir):
    return build_silver(raw_dir=raw_dir, out_dir=out_dir, source_type="api")
