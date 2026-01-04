from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

import httpx


@dataclass(frozen=True)
class WpPost:
    id: int
    date: str
    link: str
    title: str
    content_html: str


_URL_RE = re.compile(r"https?://[^\s\"']+")


def extract_urls(html: str) -> list[str]:
    return _URL_RE.findall(html or "")


def fetch_posts(
    *,
    base_url: str,
    search: str,
    per_page: int = 50,
    max_pages: int = 30,
    timeout_s: float = 60,
    user_agent: str,
) -> list[WpPost]:
    posts: list[WpPost] = []

    headers = {"User-Agent": user_agent}
    with httpx.Client(timeout=timeout_s, headers=headers, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = f"{base_url}/posts"
            params = {"search": search, "per_page": per_page, "page": page}
            resp = client.get(url, params=params)
            if resp.status_code == 400:
                # Usually "rest_post_invalid_page_number".
                break
            resp.raise_for_status()
            data: list[dict[str, Any]] = resp.json()
            if not data:
                break

            for item in data:
                posts.append(
                    WpPost(
                        id=int(item["id"]),
                        date=str(item.get("date", "")),
                        link=str(item.get("link", "")),
                        title=str(item.get("title", {}).get("rendered", "")),
                        content_html=str(item.get("content", {}).get("rendered", "")),
                    )
                )

    # Deduplicate by id.
    seen: set[int] = set()
    out: list[WpPost] = []
    for p in posts:
        if p.id in seen:
            continue
        seen.add(p.id)
        out.append(p)
    return out


def fetch_posts_multi(
    *,
    base_url: str,
    search_terms: Iterable[str],
    per_page: int = 50,
    max_pages: int = 30,
    timeout_s: float = 60,
    user_agent: str,
) -> list[WpPost]:
    all_posts: list[WpPost] = []
    for term in search_terms:
        all_posts.extend(
            fetch_posts(
                base_url=base_url,
                search=term,
                per_page=per_page,
                max_pages=max_pages,
                timeout_s=timeout_s,
                user_agent=user_agent,
            )
        )

    # Deduplicate by id.
    by_id: dict[int, WpPost] = {p.id: p for p in all_posts}
    return list(by_id.values())
