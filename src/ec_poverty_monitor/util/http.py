from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx


@dataclass(frozen=True)
class Downloaded:
    url: str
    path: Path
    bytes: int
    sha256: str
    response_headers: dict[str, str]


def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def download(
    *,
    url: str,
    out_dir: Path,
    user_agent: str,
    timeout_s: float = 60,
    force: bool = False,
) -> Downloaded:
    out_dir.mkdir(parents=True, exist_ok=True)
    key = _hash_url(url)
    bin_path = out_dir / f"{key}.bin"
    meta_path = out_dir / f"{key}.json"

    if bin_path.exists() and meta_path.exists() and not force:
        meta = json.loads(meta_path.read_text())
        return Downloaded(
            url=url,
            path=bin_path,
            bytes=int(meta["bytes"]),
            sha256=str(meta["sha256"]),
            response_headers=dict(meta.get("response_headers", {})),
        )

    headers = {"User-Agent": user_agent}
    with httpx.Client(follow_redirects=True, timeout=timeout_s, headers=headers) as client:
        resp = client.get(url)
        resp.raise_for_status()

    content = resp.content
    sha = _sha256_bytes(content)
    bin_path.write_bytes(content)

    response_headers = {k: v for k, v in resp.headers.items()}
    meta: dict[str, Any] = {
        "url": url,
        "bytes": len(content),
        "sha256": sha,
        "response_headers": response_headers,
    }
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))

    return Downloaded(url=url, path=bin_path, bytes=len(content), sha256=sha, response_headers=response_headers)
