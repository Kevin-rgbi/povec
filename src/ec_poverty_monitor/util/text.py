from __future__ import annotations

import re


def parse_float_maybe(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if s in {"", "-", "—", "..", "…"}:
        return None

    # Remove thousands separators and normalize decimal comma.
    s = s.replace("\u00a0", " ")
    s = s.replace(".", "") if re.search(r"\d\.\d{3}(\D|$)", s) else s
    s = s.replace(",", ".")

    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    return float(m.group(0))
