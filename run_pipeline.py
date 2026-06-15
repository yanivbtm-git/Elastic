#!/usr/bin/env python3
"""Automated AI job search pipeline.

Pipeline stages:
1. search_jobs()      -> Gemini + web search
2. ats_scraper()      -> Greenhouse / Lever / Ashby public APIs
3. score_jobs()       -> Gemini scoring
4. filter_jobs()      -> score >= 4 + location match + posted <= 30 days
5. save               -> scored_jobs.json (single source of truth)
6. build              -> docs/index.html from template
7. git push           -> optional auto push for CI
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import html
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
PROFILE_PATH = ROOT / "profile.md"
SCORED_JOBS_PATH = ROOT / "scored_jobs.json"
TEMPLATE_PATH = ROOT / "templates" / "index.template.html"
DOCS_INDEX_PATH = ROOT / "docs" / "index.html"

DEFAULT_SEARCH_PROMPT = (
    "Find CURRENTLY OPEN jobs (posted last 30 days) for: {roles}. "
    "Each URL must be a DIRECT link to a specific posting. Return 8-15 as JSON: "
    "{company, title, location, url, posted, description}"
)
DEFAULT_SCORE_PROMPT = (
    "Evaluate this job for [profile]. Return JSON only: "
    "{fit_score 1-10, score_reason, ai_opener, location_ok}. If location not OK -> 0."
)

USER_STATE_FIELDS = {
    "status",
    "notes",
    "interview_rounds",
    "status_history",
    "initial_status",
    "saved_at",
    "added_at",
    "manual",
}

ALLOWED_PROFILE_SECTIONS = (
    "professional summary",
    "core strengths",
    "skills snapshot",
    "relevant experience highlights",
    "certifications and education",
)


def _log(msg: str) -> None:
    print(f"[pipeline] {msg}")


def _utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _http_get(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def _http_post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: int = 45,
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    merged_headers = {"content-type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=data, headers=merged_headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _extract_json_block(raw: str) -> Any:
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\n", "", text)
        text = re.sub(r"\n```$", "", text)

    decoder = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch not in "{[":
            continue
        try:
            obj, _ = decoder.raw_decode(text[i:])
            return obj
        except json.JSONDecodeError:
            continue
    raise ValueError("No valid JSON object/array found in model output")


def _clean_html(raw: str, max_len: int = 900) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def _parse_date(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Heuristic: values > 10^11 are milliseconds.
        stamp = float(value)
        if stamp > 1e11:
            stamp = stamp / 1000.0
        try:
            return dt.datetime.fromtimestamp(stamp, tz=dt.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except ValueError:
        pass

    formats = (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%m/%d/%Y",
    )
    for fmt in formats:
        try:
            parsed = dt.datetime.strptime(text, fmt).replace(tzinfo=dt.timezone.utc)
            return parsed
        except ValueError:
            continue
    return None


def _normalize_posted(value: Any) -> str:
    parsed = _parse_date(value)
    if parsed is None:
        return dt.datetime.now(dt.timezone.utc).date().isoformat()
    return parsed.date().isoformat()


def _normalize_url(url: str) -> str:
    text = (url or "").strip()
    if not text:
        return ""
    if not re.match(r"^https?://", text, flags=re.IGNORECASE):
        text = f"https://{text}"
    return text


def _location_ok(location: str, prefs: dict[str, Any]) -> bool:
    if not location:
        return False
    loc = location.lower()
    israel_only = bool(prefs.get("israel_only", False))
    if not israel_only:
        return True

    israel_tokens = (
        "israel",
        "tel aviv",
        "tel-aviv",
        "jerusalem",
        "haifa",
        "petah tikva",
        "center district",
        "centre district",
        "gush dan",
        "raanana",
        "netanya",
        "rishon lezion",
        "herzliya",
    )
    remote_tokens = ("remote", "work from home", "hybrid")

    if any(tok in loc for tok in israel_tokens):
        return True
    if "remote" in loc and "israel" in loc:
        return True
    if any(tok in loc for tok in remote_tokens):
        # Accept generic remote only when no conflicting region markers exist.
        blocked = ("usa", "united states", "europe", "uk", "canada", "india", "latam")
        return not any(tok in loc for tok in blocked)
    return False


def _title_matches_roles(title: str, roles: list[str]) -> bool:
    normalized_title = re.sub(r"[^a-z0-9\s]", " ", (title or "").lower())
    normalized_title = re.sub(r"\s+", " ", normalized_title).strip()
    if not normalized_title:
        return False

    # Canonical role families + common recruiter phrasing.
    role_signals: dict[str, tuple[str, ...]] = {
        "project manager": (
            "project manager",
            "program manager",
            "delivery manager",
            "implementation manager",
            "technical project manager",
            "pmo",
            "project lead",
        ),
        "customer success": (
            "customer success",
            "customer success manager",
            "csm",
            "technical account manager",
            "customer account manager",
            "client success",
            "customer experience manager",
        ),
        "customer architect": (
            "customer architect",
            "solutions architect",
            "solution architect",
            "customer solutions architect",
            "enterprise architect",
            "technical architect",
        ),
    }

    requested = [r.lower().strip() for r in roles if r and r.strip()]
    active_signals: set[str] = set()
    for role in requested:
        if role in role_signals:
            active_signals.update(role_signals[role])
        else:
            active_signals.add(role)

    if not active_signals:
        return False
    return any(signal in normalized_title for signal in active_signals)


def _job_id(job: dict[str, Any]) -> str:
    key = (job.get("url") or "") + "|" + (job.get("title") or "") + "|" + (job.get("company") or "")
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        _log(f"Failed to parse JSON from {path}, using fallback.")
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _redact_pii(text: str) -> str:
    redacted = text
    redacted = re.sub(r"[\w.+-]+@[\w.-]+\.\w+", "[redacted-email]", redacted, flags=re.IGNORECASE)
    redacted = re.sub(r"\+?\d[\d\s().-]{7,}\d", "[redacted-phone]", redacted)
    return redacted


def _extract_safe_profile(full_profile_text: str) -> str:
    lines = full_profile_text.splitlines()
    sections: dict[str, list[str]] = {}
    current: str | None = None

    for raw_line in lines:
        line = raw_line.rstrip()
        heading = None
        if line.startswith("## "):
            heading = line[3:].strip().lower()
        elif line.startswith("# "):
            heading = line[2:].strip().lower()

        if heading is not None:
            current = heading
            sections.setdefault(current, [])
            continue
        if current is not None:
            sections[current].append(line)

    selected_chunks: list[str] = []
    for sec_name in ALLOWED_PROFILE_SECTIONS:
        body_lines = sections.get(sec_name, [])
        body = "\n".join(body_lines).strip()
        if body:
            selected_chunks.append(f"## {sec_name.title()}\n{body}")

    if not selected_chunks:
        # Fallback when headings are missing: strip obvious PII and keep content.
        selected_chunks = [full_profile_text]

    safe_text = "\n\n".join(selected_chunks).strip()
    safe_text = _redact_pii(safe_text)
    # Drop any explicit contact lines that slipped in.
    safe_lines = [
        ln for ln in safe_text.splitlines()
        if not re.search(r"(phone|email|@)", ln, flags=re.IGNORECASE)
    ]
    safe_text = "\n".join(safe_lines).strip()
    return safe_text


def _privacy_settings(config: dict[str, Any]) -> dict[str, Any]:
    defaults = {
        "redact_personal_data": True,
        "pipeline_profile_mode": "summary_only",
        "ui_full_profile_opt_in_default": False,
    }
    cfg = config.get("privacy", {})
    if not isinstance(cfg, dict):
        return defaults
    return {**defaults, **cfg}


def _gemini_generate(
    model: str,
    prompt: str,
    temperature: float = 0.2,
    max_tokens: int = 1400,
) -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    endpoint = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{urllib.parse.quote(model, safe='')}:"  # model name in path
        f"generateContent?key={urllib.parse.quote(api_key, safe='')}"
    )
    payload: dict[str, Any] = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
        },
    }
    data = _http_post_json(endpoint, payload, headers={"content-type": "application/json"})
    texts: list[str] = []
    for candidate in data.get("candidates", []):
        parts = ((candidate or {}).get("content") or {}).get("parts", [])
        for part in parts:
            text = (part or {}).get("text", "")
            if text:
                texts.append(text)
    return "\n".join(texts).strip()


def web_search(query: str, max_results: int = 10) -> list[dict[str, str]]:
    encoded = urllib.parse.quote_plus(query)
    url = f"https://duckduckgo.com/html/?q={encoded}"
    headers = {"user-agent": "Mozilla/5.0 (JobSearchBot/1.0)"}
    try:
        html_doc = _http_get(url, headers=headers, timeout=30)
    except urllib.error.URLError as exc:
        _log(f"web_search failed for query '{query}': {exc}")
        return []

    anchors = re.findall(
        r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        html_doc,
        flags=re.IGNORECASE | re.DOTALL,
    )
    snippets = re.findall(
        r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>|<div[^>]*class="result__snippet"[^>]*>(.*?)</div>',
        html_doc,
        flags=re.IGNORECASE | re.DOTALL,
    )

    results: list[dict[str, str]] = []
    seen: set[str] = set()
    for idx, (href, title_html) in enumerate(anchors):
        if len(results) >= max_results:
            break
        parsed_href = urllib.parse.unquote(href)
        # DuckDuckGo redirect URLs commonly include "uddg=".
        if "uddg=" in parsed_href:
            parsed_href = urllib.parse.parse_qs(urllib.parse.urlparse(parsed_href).query).get("uddg", [""])[0]
        parsed_href = _normalize_url(parsed_href)
        if not parsed_href or parsed_href in seen:
            continue
        seen.add(parsed_href)

        snippet_raw = ""
        if idx < len(snippets):
            snippet_raw = snippets[idx][0] or snippets[idx][1]
        title = _clean_html(title_html, max_len=150)
        snippet = _clean_html(snippet_raw, max_len=300)
        results.append({"title": title, "url": parsed_href, "snippet": snippet})
    return results


def _heuristic_jobs_from_hits(hits: list[dict[str, str]], roles: list[str], location: str) -> list[dict[str, Any]]:
    role_tokens = [r.lower() for r in roles]
    jobs: list[dict[str, Any]] = []
    for hit in hits:
        url = hit.get("url", "")
        title = hit.get("title", "")
        title_lower = title.lower()
        if role_tokens and not any(token in title_lower for token in role_tokens):
            continue

        domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
        company = domain.split(".")[0].replace("-", " ").title() if domain else "Unknown"
        jobs.append(
            {
                "company": company,
                "title": title or "Untitled role",
                "location": location,
                "url": url,
                "posted": dt.date.today().isoformat(),
                "description": hit.get("snippet", ""),
                "source": "web_search_heuristic",
            }
        )
    return jobs


def search_jobs(config: dict[str, Any]) -> list[dict[str, Any]]:
    candidate = config.get("candidate", {})
    roles = candidate.get("target_roles", [])
    if not roles:
        return []

    location_scope = candidate.get("location_preferences", {}).get("country", "Israel")
    web_hits: list[dict[str, str]] = []
    for role in roles:
        query = (
            f"{role} {location_scope} open role posted in last month "
            "site:greenhouse.io OR site:jobs.lever.co OR site:ashbyhq.com"
        )
        web_hits.extend(web_search(query, max_results=8))
        time.sleep(0.3)

    # Deduplicate URL-level hits.
    dedup_hits: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for item in web_hits:
        url = item.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        dedup_hits.append(item)

    prompt_template = (
        config.get("ai_prompts", {}).get("search_prompt_template", DEFAULT_SEARCH_PROMPT)
    )
    # Prompt text includes literal JSON braces, so we avoid str.format.
    main_prompt = prompt_template.replace("{roles}", ", ".join(roles))
    if not os.getenv("GEMINI_API_KEY"):
        _log("GEMINI_API_KEY missing; using heuristic search extraction.")
        return _heuristic_jobs_from_hits(dedup_hits, roles, location_scope)

    model = os.getenv("GEMINI_PRO_MODEL", "gemini-1.5-pro-latest")
    prompt = (
        f"{main_prompt}\n\n"
        "Use these web search candidates as context and only keep likely direct posting URLs.\n"
        f"Location preference: {location_scope}.\n"
        f"Candidates JSON:\n{json.dumps(dedup_hits, ensure_ascii=False)}"
    )
    try:
        raw = _gemini_generate(model=model, prompt=prompt, temperature=0.1, max_tokens=2200)
        parsed = _extract_json_block(raw)
        if isinstance(parsed, dict):
            parsed = parsed.get("jobs", [])
        if not isinstance(parsed, list):
            raise ValueError("Expected list from Opus search output")
    except Exception as exc:  # pylint: disable=broad-except
        _log(f"Gemini search failed, falling back to heuristics: {exc}")
        return _heuristic_jobs_from_hits(dedup_hits, roles, location_scope)

    normalized: list[dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "company": (item.get("company") or "Unknown").strip(),
                "title": (item.get("title") or "Untitled role").strip(),
                "location": (item.get("location") or location_scope).strip(),
                "url": _normalize_url(item.get("url") or ""),
                "posted": _normalize_posted(item.get("posted")),
                "description": _clean_html(item.get("description", ""), max_len=1200),
                "source": "opus_web_search",
            }
        )
    return [j for j in normalized if j.get("url")]


def _fetch_greenhouse(board: str) -> list[dict[str, Any]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    try:
        payload = json.loads(_http_get(url, timeout=35))
    except Exception as exc:  # pylint: disable=broad-except
        _log(f"Greenhouse fetch failed ({board}): {exc}")
        return []

    jobs: list[dict[str, Any]] = []
    for job in payload.get("jobs", []):
        jobs.append(
            {
                "company": board.replace("-", " ").title(),
                "title": job.get("title", "Untitled role"),
                "location": (job.get("location") or {}).get("name", "Israel"),
                "url": _normalize_url(job.get("absolute_url", "")),
                "posted": _normalize_posted(job.get("updated_at") or job.get("first_published")),
                "description": _clean_html(job.get("content", ""), max_len=1300),
                "source": "greenhouse",
            }
        )
    return jobs


def _fetch_lever(company: str) -> list[dict[str, Any]]:
    url = f"https://api.lever.co/v0/postings/{company}?mode=json"
    try:
        payload = json.loads(_http_get(url, timeout=35))
    except Exception as exc:  # pylint: disable=broad-except
        _log(f"Lever fetch failed ({company}): {exc}")
        return []

    jobs: list[dict[str, Any]] = []
    for job in payload:
        categories = job.get("categories", {}) if isinstance(job, dict) else {}
        jobs.append(
            {
                "company": company.replace("-", " ").title(),
                "title": job.get("text", "Untitled role"),
                "location": categories.get("location", "Israel"),
                "url": _normalize_url(job.get("hostedUrl", "")),
                "posted": _normalize_posted(job.get("createdAt")),
                "description": _clean_html(job.get("descriptionPlain", "") or job.get("description", ""), max_len=1300),
                "source": "lever",
            }
        )
    return jobs


ASHBY_QUERY = """
query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
    jobs {
      id
      title
      locationName
      employmentType
      publishedAt
      updatedAt
      applyUrl
      descriptionHtml
    }
  }
}
"""


def _fetch_ashby(organization_slug: str) -> list[dict[str, Any]]:
    endpoint = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
    payload = {
        "operationName": "ApiJobBoardWithTeams",
        "query": ASHBY_QUERY,
        "variables": {"organizationHostedJobsPageName": organization_slug},
    }
    try:
        data = _http_post_json(endpoint, payload, timeout=40)
    except Exception as exc:  # pylint: disable=broad-except
        _log(f"Ashby fetch failed ({organization_slug}): {exc}")
        return []

    jobs_raw = (
        data.get("data", {})
        .get("jobBoardWithTeams", {})
        .get("jobs", [])
    )
    jobs: list[dict[str, Any]] = []
    for job in jobs_raw:
        if not isinstance(job, dict):
            continue
        jobs.append(
            {
                "company": organization_slug.replace("-", " ").title(),
                "title": job.get("title", "Untitled role"),
                "location": job.get("locationName", "Israel"),
                "url": _normalize_url(job.get("applyUrl", "")),
                "posted": _normalize_posted(job.get("publishedAt") or job.get("updatedAt")),
                "description": _clean_html(job.get("descriptionHtml", ""), max_len=1300),
                "source": "ashby",
            }
        )
    return jobs


def ats_scraper(config: dict[str, Any]) -> list[dict[str, Any]]:
    ats_cfg = config.get("ats_sources", {})
    greenhouse_boards = ats_cfg.get("greenhouse_boards", [])
    lever_companies = ats_cfg.get("lever_companies", [])
    ashby_orgs = ats_cfg.get("ashby_organizations", [])

    jobs: list[dict[str, Any]] = []
    for board in greenhouse_boards:
        jobs.extend(_fetch_greenhouse(board))
    for company in lever_companies:
        jobs.extend(_fetch_lever(company))
    for org in ashby_orgs:
        jobs.extend(_fetch_ashby(org))
    return [j for j in jobs if j.get("url")]


def _fallback_score(job: dict[str, Any], roles: list[str], prefs: dict[str, Any]) -> dict[str, Any]:
    corpus = f"{job.get('title', '')} {job.get('description', '')}".lower()
    role_hits = sum(1 for role in roles if role.lower() in corpus)
    score = min(10, 3 + (2 * role_hits))

    location_ok = _location_ok(job.get("location", ""), prefs)
    if not location_ok:
        score = 0

    reason = "Keyword-based fallback score (no Gemini API key configured)."
    opener = (
        f"Your background appears relevant for {job.get('title', 'this role')} at "
        f"{job.get('company', 'this company')}."
    )
    return {
        "fit_score": score,
        "score_reason": reason,
        "ai_opener": opener,
        "location_ok": location_ok,
    }


def _score_with_gemini(
    job: dict[str, Any],
    profile_text: str,
    prompt_template: str,
    model: str,
) -> dict[str, Any]:
    prompt = (
        f"{prompt_template}\n\n"
        f"Profile:\n{profile_text}\n\n"
        f"Job:\n{json.dumps(job, ensure_ascii=False)}"
    )
    raw = _gemini_generate(model=model, prompt=prompt, temperature=0.1, max_tokens=900)
    parsed = _extract_json_block(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Expected JSON object for score payload")
    return parsed


def score_jobs(config: dict[str, Any], jobs: list[dict[str, Any]], profile_text: str) -> list[dict[str, Any]]:
    if not jobs:
        return []

    roles = config.get("candidate", {}).get("target_roles", [])
    prefs = config.get("candidate", {}).get("location_preferences", {})
    prompt_template = (
        config.get("ai_prompts", {}).get("scoring_prompt_template", DEFAULT_SCORE_PROMPT)
    )
    scored: list[dict[str, Any]] = []
    use_ai = bool(os.getenv("GEMINI_API_KEY"))
    model = os.getenv("GEMINI_FLASH_MODEL", "gemini-1.5-flash-latest")

    for idx, job in enumerate(jobs, start=1):
        payload: dict[str, Any]
        if use_ai:
            try:
                payload = _score_with_gemini(job, profile_text, prompt_template, model=model)
            except Exception as exc:  # pylint: disable=broad-except
                _log(f"Scoring fallback for job {idx}/{len(jobs)}: {exc}")
                payload = _fallback_score(job, roles, prefs)
        else:
            payload = _fallback_score(job, roles, prefs)

        fit_score = int(max(0, min(10, int(payload.get("fit_score", 0)))))
        location_ok = bool(payload.get("location_ok", _location_ok(job.get("location", ""), prefs)))
        if not location_ok:
            fit_score = 0

        enriched = copy.deepcopy(job)
        enriched["fit_score"] = fit_score
        enriched["score_reason"] = (payload.get("score_reason") or "").strip()
        enriched["ai_opener"] = (payload.get("ai_opener") or "").strip()
        enriched["location_ok"] = location_ok
        enriched["updated_at"] = _utcnow_iso()
        enriched["job_id"] = _job_id(enriched)
        scored.append(enriched)
    return scored


def filter_jobs(config: dict[str, Any], jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filt = config.get("pipeline", {}).get("filters", {})
    min_score = int(filt.get("minimum_fit_score", 4))
    days_window = int(filt.get("posted_within_days", 30))
    require_location = bool(filt.get("require_location_match", True))
    require_role_title_match = bool(filt.get("require_role_title_match", True))
    roles = config.get("candidate", {}).get("target_roles", [])
    now = dt.datetime.now(dt.timezone.utc)

    filtered: list[dict[str, Any]] = []
    for job in jobs:
        if require_role_title_match and not _title_matches_roles(job.get("title", ""), roles):
            continue
        if int(job.get("fit_score", 0)) < min_score:
            continue
        if require_location and not bool(job.get("location_ok", False)):
            continue
        posted_dt = _parse_date(job.get("posted"))
        if posted_dt is None:
            continue
        if (now - posted_dt).days > days_window:
            continue
        filtered.append(job)
    return filtered


def merge_jobs(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url_existing = {item.get("url"): item for item in existing if item.get("url")}
    merged_by_url: dict[str, dict[str, Any]] = {}

    for new_job in incoming:
        url = new_job.get("url")
        if not url:
            continue
        merged = copy.deepcopy(new_job)
        prev = by_url_existing.get(url, {})
        for key in USER_STATE_FIELDS:
            if key in prev and key not in merged:
                merged[key] = copy.deepcopy(prev[key])
            elif key in prev and key in ("notes", "interview_rounds", "status_history"):
                merged[key] = copy.deepcopy(prev[key])
        merged.setdefault("status", prev.get("status", prev.get("initial_status", "saved")))
        merged.setdefault("added_at", prev.get("added_at", _utcnow_iso()))
        merged_by_url[url] = merged

    # Critical rule: never delete manual jobs carrying initial_status.
    for old in existing:
        url = old.get("url")
        if not url:
            continue
        if "initial_status" in old and url not in merged_by_url:
            carry = copy.deepcopy(old)
            carry["preserved_manual"] = True
            carry["updated_at"] = _utcnow_iso()
            merged_by_url[url] = carry

    merged = list(merged_by_url.values())
    merged.sort(
        key=lambda j: (
            int(j.get("fit_score", 0)),
            _normalize_posted(j.get("posted")),
            j.get("company", ""),
        ),
        reverse=True,
    )
    return merged


def build_docs(config: dict[str, Any], jobs: list[dict[str, Any]], profile_text: str, safe_profile_text: str) -> None:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Missing template file: {TEMPLATE_PATH}")
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    output = (
        template.replace("__INITIAL_JOBS_JSON__", json.dumps(jobs, ensure_ascii=False))
        .replace("__PROFILE_TEXT__", json.dumps(profile_text))
        .replace("__PROFILE_SAFE_TEXT__", json.dumps(safe_profile_text))
        .replace("__UI_PROFILE_OPT_IN_DEFAULT__", json.dumps(_privacy_settings(config).get("ui_full_profile_opt_in_default", False)))
        .replace("__GENERATED_AT__", _utcnow_iso())
        .replace(
            "__CANDIDATE_NAME__",
            str(config.get("candidate", {}).get("name", "Candidate")),
        )
    )
    DOCS_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOCS_INDEX_PATH.write_text(output, encoding="utf-8")


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)


def auto_git_push(commit_message: str) -> None:
    _run(["git", "add", str(SCORED_JOBS_PATH), str(DOCS_INDEX_PATH)])
    diff = _run(["git", "diff", "--cached", "--quiet"])
    if diff.returncode == 0:
        _log("No pipeline output changes to commit.")
        return

    commit = _run(["git", "commit", "-m", commit_message])
    if commit.returncode != 0:
        raise RuntimeError(f"git commit failed: {commit.stderr}")

    branch = _run(["git", "branch", "--show-current"]).stdout.strip()
    push = _run(["git", "push", "-u", "origin", branch])
    if push.returncode != 0:
        raise RuntimeError(f"git push failed: {push.stderr}")
    _log(f"Pushed pipeline updates to {branch}.")


def dedupe_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for job in jobs:
        url = job.get("url", "").strip()
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        result.append(job)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run AI job search pipeline.")
    parser.add_argument("--auto-push", action="store_true", help="Commit and push generated outputs.")
    args = parser.parse_args()

    config = load_json(CONFIG_PATH, default={})
    profile_text = PROFILE_PATH.read_text(encoding="utf-8") if PROFILE_PATH.exists() else ""
    safe_profile_text = _extract_safe_profile(profile_text)
    privacy = _privacy_settings(config)
    use_safe_profile = bool(privacy.get("redact_personal_data", True))
    if privacy.get("pipeline_profile_mode") == "summary_only" and use_safe_profile:
        score_profile_text = safe_profile_text
        _log("Privacy mode enabled: scoring with safe profile summary only.")
    else:
        score_profile_text = _redact_pii(profile_text) if use_safe_profile else profile_text
    existing_jobs = load_json(SCORED_JOBS_PATH, default=[])
    if not isinstance(existing_jobs, list):
        existing_jobs = []

    _log("Running search_jobs() ...")
    search_results = search_jobs(config)
    _log(f"Found {len(search_results)} jobs via search.")

    _log("Running ats_scraper() ...")
    ats_results = ats_scraper(config)
    _log(f"Found {len(ats_results)} jobs via ATS APIs.")

    discovered = dedupe_jobs(search_results + ats_results)
    _log(f"Discovered {len(discovered)} unique jobs.")

    _log("Running score_jobs() ...")
    scored = score_jobs(config, discovered, score_profile_text)
    _log(f"Scored {len(scored)} jobs.")

    _log("Running filter_jobs() ...")
    filtered = filter_jobs(config, scored)
    _log(f"Filtered down to {len(filtered)} jobs.")

    merged = merge_jobs(existing_jobs, filtered)
    _log(f"Merged jobs total: {len(merged)} (including preserved manual entries).")

    save_json(SCORED_JOBS_PATH, merged)
    _log(f"Saved source of truth: {SCORED_JOBS_PATH}")

    build_docs(config, merged, profile_text, safe_profile_text)
    _log(f"Built tracker: {DOCS_INDEX_PATH}")

    if args.auto_push or os.getenv("AUTO_GIT_PUSH") == "1":
        auto_git_push(f"chore: refresh scored jobs ({dt.date.today().isoformat()})")

    _log("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
