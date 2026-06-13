"""
01_findingaid_json_extractor_arclight.py

Extracts collection-level metadata and component records from an Arclight
instance for a list of finding aid EAD IDs.

Reads IDs from a plain text file (one ID per line) or a CSV file
(first column used as IDs, header row skipped if present).

Outputs one slim JSON file per collection containing:
  - collection:  full raw.json Solr document
  - series_ids:  ids of series-level components (records live in 'components')
  - components:  flattened list of ALL component records, each with a
                 'breadcrumb' string (e.g. "Collection: X → Series: Y → File: Z");
                 with --enrich each also carries containers, dates, and scope content

Component records are flattened from the verbose JSON-API shape: attribute
wrappers reduced to plain values, "true"/"false" strings coerced to booleans,
trailing '?' dropped from key names, and 'links' omitted (derivable from id).

Usage:
    python 01_findingaid_json_extractor_arclight.py --ids finding_aid_ids.txt --out output/
    python 01_findingaid_json_extractor_arclight.py --ids finding_aid_ids.csv --out output/ --csv
    python 01_findingaid_json_extractor_arclight.py --ids finding_aid_ids.txt --series-only
    python 01_findingaid_json_extractor_arclight.py --ids finding_aid_ids.txt --enrich
    python 01_findingaid_json_extractor_arclight.py --ids finding_aid_ids.txt --enrich --rate-limit 0.5

Options:
    --out OUTPUT        Output directory (default: arclight_output/)
    --csv               Treat --ids file as CSV (first column used)
    --series-only       Only fetch series-level records, skip full component list
    --enrich            Fetch raw.json per component to add containers, dates, and
                        scope content; adds one HTTP request per component (slow).
                        Progress is checkpointed to {out}/enrich-cache/ after every
                        fetch, so an interrupted run resumes where it left off;
                        the cache file is removed once the collection saves.
    --skip-existing     Skip collections whose output JSON already exists (makes
                        interrupted or repeated runs cheap)
    --rate-limit SECS   Minimum seconds between HTTP requests (default: 1.0); time
                        spent waiting on the server counts toward the interval;
                        1.0 is a reasonable minimum, though 0.5 likely the 
                        low threshold; --enrich multiplies this by
                        the component count
"""

import argparse
import csv
import html
import json
import logging
import time
import sys
from pathlib import Path

import requests

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_URL   = "https://findingaids.lib.umich.edu"
RATE_LIMIT = 1.0   # minimum seconds between requests (be polite)
PER_PAGE   = 100   # Arclight max is 100

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "findingaid-extractor/1.1 (research data pipeline; contact: jajohnst@umich.edu)",
    "Accept": "application/json",
})

# parent_ssim and level_ssm omitted: identical to the parent_ids and level
# already present in every component record
ENRICH_FIELDS = [
    "containers_ssim",
    "unitdate_ssm",
    "unitdate_inclusive_ssm",
    "normalized_date_ssm",
    "scopecontent_tesim",
]

# ── Logging ───────────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)


def setup_logging(log_path: Path) -> None:
    """Write all log output to both the terminal and a log file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(fmt)

    logging.basicConfig(level=logging.INFO, handlers=[console, file_handler])
    log.info(f"Logging to {log_path}")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _attr_val(field):
    """Extract .attributes.value from a JSON-API attribute object, or return as-is."""
    if isinstance(field, dict):
        return field.get("attributes", {}).get("value")
    return field


def flatten_component(comp: dict) -> dict:
    """
    Reduce a verbose JSON-API component record to a flat dict of plain values.

    Attribute wrappers become their values, "true"/"false" strings become
    booleans (trailing '?' dropped from key names), and 'links'/'type' are
    omitted (derivable from id / duplicated by level).
    """
    slim = {"id": comp["id"]}
    for key, field in comp.get("attributes", {}).items():
        val = _attr_val(field)
        if key.endswith("?"):
            slim[key.rstrip("?")] = val == "true" if isinstance(val, str) else bool(val)
        else:
            slim[key] = val
    return slim


_last_request_time = 0.0


def get(url: str, params: dict = None) -> dict:
    """GET with basic error handling and rate limiting.

    Sleeps only the remainder of RATE_LIMIT since the previous request,
    so time spent waiting on the server counts toward the interval.
    """
    global _last_request_time
    wait = RATE_LIMIT - (time.monotonic() - _last_request_time)
    if wait > 0:
        time.sleep(wait)
    _last_request_time = time.monotonic()
    try:
        r = SESSION.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        log.error(f"HTTP error {r.status_code}: {url}")
        raise
    except Exception as e:
        log.error(f"Request failed: {e}")
        raise


def fetch_collection(ead_id: str) -> dict | None:
    """Fetch the raw Solr document for a collection root record."""
    url = f"{BASE_URL}/catalog/{ead_id}/raw.json"
    log.info(f"Fetching collection: {url}")
    try:
        return get(url)
    except Exception:
        log.warning(f"Skipping {ead_id} — could not fetch collection record.")
        return None


def load_enrich_cache(cache_path: Path) -> dict:
    """Load a checkpoint cache of already-enriched components: id -> fields dict."""
    cached = {}
    if cache_path.exists():
        for line in cache_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                cached[entry["id"]] = entry["fields"]
            except (json.JSONDecodeError, KeyError):
                # a torn final line from an interrupted write — refetch that one
                continue
    return cached


def enrich_components(components: list[dict], cache_path: Path) -> None:
    """
    Fetch raw.json per component and merge containers, dates, and scope content in-place.

    Slow: one HTTP request per component. Use --enrich for large collections with care.

    Checkpointing: each successful fetch is appended to cache_path immediately,
    so an interrupted run resumes where it left off. Failed fetches are not
    cached and are retried on the next run. The caller deletes the cache file
    once the collection's output JSON is safely saved.
    """
    cached = load_enrich_cache(cache_path)
    if cached:
        log.info(f"Restored {len(cached)} enriched components from cache: {cache_path}")

    total = len(components)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a", encoding="utf-8") as cache_file:
        for i, comp in enumerate(components, 1):
            cid = comp["id"]
            if cid in cached:
                comp.update(cached[cid])
                continue
            url = f"{BASE_URL}/catalog/{cid}/raw.json"
            log.info(f"Enriching {i}/{total}: {cid}")
            try:
                raw = get(url)
                fields = {k: raw[k] for k in ENRICH_FIELDS if k in raw}
            except Exception:
                log.warning(f"Skipping enrichment for {cid}")
                continue
            comp.update(fields)
            cache_file.write(json.dumps({"id": cid, "fields": fields}, ensure_ascii=False) + "\n")
            cache_file.flush()


def build_breadcrumbs(result: dict) -> None:
    """
    Add a 'breadcrumb' string to every component, assembled from the hierarchy.

    Built from data already in the extracted components — no HTTP calls.
    Format: "Collection: Title → Series: Title → File: Title"

    parent_ids come back as bare ids (e.g. 'aspace_28c2...') while component
    ids are prefixed ('umich-bhl-9622_aspace_28c2...'), so lookups fall back
    to the prefixed form.
    """
    ead_id = result["ead_id"]
    lookup = {}  # component_id -> (level, display_title)

    coll = result["collection"]
    coll_title = coll.get("title_ssm", [""])[0] if isinstance(coll.get("title_ssm"), list) else coll.get("title_ssm", "")
    lookup[ead_id] = ("Collection", html.unescape(coll_title))

    for comp in result["components"]:
        level = comp.get("level") or "Component"
        title = comp.get("title") or comp.get("normalized_title") or comp["id"]
        lookup[comp["id"]] = (level, html.unescape(title))

    for comp in result["components"]:
        parent_ids = comp.get("parent_ids") or []
        if isinstance(parent_ids, str):
            parent_ids = [parent_ids]

        parts = []
        for pid in parent_ids:
            if pid not in lookup:
                pid = f"{ead_id}_{pid}"
            if pid in lookup:
                parts.append(f"{lookup[pid][0]}: {lookup[pid][1]}")
            else:
                log.warning(f"Breadcrumb ancestor not found for {comp['id']}: {pid}")

        level, title = lookup[comp["id"]]
        parts.append(f"{level}: {title}")

        comp["breadcrumb"] = " → ".join(parts)


def fetch_components(ead_id: str, level_filter: str = None) -> list[dict]:
    """
    Fetch all component records for a collection, paginated.

    level_filter: optionally restrict to a single level, e.g. 'Series'
    Returns a flat list of JSON-API data objects.
    """
    components = []
    page = 1

    while True:
        params = {
            "f[parent_ssim][]": ead_id,
            "per_page": PER_PAGE,
            "page": page,
        }
        if level_filter:
            params["f[level_sim][]"] = level_filter

        url = f"{BASE_URL}/catalog.json"
        log.info(f"Fetching components page {page}: {ead_id}" +
                 (f" [{level_filter}]" if level_filter else ""))

        data = get(url, params=params)
        records = data.get("data", [])
        components.extend(records)

        pages = data.get("meta", {}).get("pages", {})
        total_pages = pages.get("total_pages", 1)
        total_count = pages.get("total_count", 0)

        log.info(f"  → {len(records)} records (page {page}/{total_pages}, "
                 f"total: {total_count})")

        if page >= total_pages:
            break
        page += 1

    return components


# ── ID loading ─────────────────────────────────────────────────────────────────

def load_ids_txt(path: Path) -> list[str]:
    """Load EAD IDs from a plain text file (one per line)."""
    ids = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            ids.append(line)
    return ids


def load_ids_csv(path: Path) -> list[str]:
    """Load EAD IDs from the first column of a CSV file."""
    ids = []
    with path.open() as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            val = row[0].strip()
            # skip header if first row looks like a label
            if i == 0 and not val.startswith("umich-"):
                log.info(f"Skipping header row: {val}")
                continue
            if val:
                ids.append(val)
    return ids


# ── Core extraction ────────────────────────────────────────────────────────────

def extract_collection(ead_id: str, out_dir: Path, series_only: bool = False,
                       enrich: bool = False) -> dict | None:
    """
    Extract all data for one finding aid.

    Returns a dict with keys:
      ead_id, collection, series_ids, components

    Components are flattened to plain values. Each gets a 'breadcrumb' string.
    With enrich=True, each also carries containers, dates, and scope content
    from raw.json, checkpointed to {out_dir}/enrich-cache/ so interrupted runs
    resume automatically. series_ids is derived locally from the component
    list — no separate series request is made.
    """
    log.info(f"{'─' * 60}")
    log.info(f"Processing: {ead_id}")

    # 1. Collection-level record
    collection = fetch_collection(ead_id)
    if collection is None:
        return None

    # 2. Component records — one fetch; series derived locally by level
    if series_only:
        raw_comps = fetch_components(ead_id, level_filter="Series")
    else:
        raw_comps = fetch_components(ead_id)
    components = [flatten_component(c) for c in raw_comps]

    result = {
        "ead_id": ead_id,
        "collection": collection,
        "series_ids": [c["id"] for c in components if c.get("level") == "Series"],
        "components": components,
    }

    # 3. Breadcrumbs — built from data already in hand, no extra requests
    if components:
        build_breadcrumbs(result)

    # 4. Per-component enrichment (containers, dates, scope) — one request per
    #    component, checkpointed so interrupted runs resume
    if enrich and components:
        log.info(f"Enriching {len(components)} components (this will be slow)...")
        cache_path = out_dir / "enrich-cache" / f"{ead_id}.jsonl"
        enrich_components(components, cache_path)

    # Summary
    log.info(f"✓ {ead_id}: {len(result['series_ids'])} series, "
             f"{len(components)} total components")

    return result


# ── Output ─────────────────────────────────────────────────────────────────────

def save(result: dict, out_dir: Path) -> None:
    """Save a result dict as a JSON file named after the EAD ID."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{result['ead_id']}.json"
    path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    log.info(f"Saved → {path}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    global RATE_LIMIT

    parser = argparse.ArgumentParser(
        description="Extract finding aid data from an Arclight instance."
    )
    parser.add_argument(
        "--ids", required=True,
        help="Path to a .txt (one ID per line) or .csv file of EAD IDs"
    )
    parser.add_argument(
        "--out", default="arclight_output",
        help="Output directory for JSON files (default: arclight_output/)"
    )
    parser.add_argument(
        "--csv", action="store_true",
        help="Treat --ids file as CSV (first column used)"
    )
    parser.add_argument(
        "--series-only", action="store_true",
        help="Only fetch series-level records, not all 148+ components"
    )
    parser.add_argument(
        "--enrich", action="store_true",
        help="Fetch raw.json per component to add containers, dates, and scope content "
             "(one extra HTTP request per component — slow for large collections)"
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip collections whose output JSON already exists in --out"
    )
    parser.add_argument(
        "--rate-limit", type=float, default=RATE_LIMIT,
        help=f"Seconds between requests (default: {RATE_LIMIT})"
    )
    args = parser.parse_args()

    RATE_LIMIT = args.rate_limit

    out_dir = Path(args.out)
    setup_logging(out_dir / "extraction.log")

    ids_path = Path(args.ids)
    if not ids_path.exists():
        log.error(f"Error: {ids_path} not found.")
        sys.exit(1)

    ids = load_ids_csv(ids_path) if args.csv else load_ids_txt(ids_path)

    if not ids:
        log.error("No IDs found in input file.")
        sys.exit(1)

    log.info(f"Loaded {len(ids)} finding aid IDs from {ids_path}")
    log.info(f"Output directory: {args.out}")
    log.info(f"Rate limit: {RATE_LIMIT}s between requests")
    mode = "series only" if args.series_only else "full components"
    if args.enrich and not args.series_only:
        mode += " + enrichment"
    log.info(f"Mode: {mode}")

    start = time.monotonic()
    failed = []

    for ead_id in ids:
        if args.skip_existing and (out_dir / f"{ead_id}.json").exists():
            log.info(f"Skipping {ead_id} — output already exists")
            continue
        try:
            result = extract_collection(ead_id, out_dir, series_only=args.series_only,
                                        enrich=args.enrich)
            if result:
                save(result, out_dir)
                # output is safe on disk — the checkpoint cache has served its purpose
                cache_path = out_dir / "enrich-cache" / f"{ead_id}.jsonl"
                cache_path.unlink(missing_ok=True)
        except Exception as e:
            log.error(f"✗ Failed: {ead_id} — {e}")
            failed.append(ead_id)

    cache_dir = out_dir / "enrich-cache"
    if cache_dir.is_dir() and not any(cache_dir.iterdir()):
        cache_dir.rmdir()

    elapsed = time.monotonic() - start
    log.info(f"{'═' * 60}")
    log.info(f"Done in {elapsed:.1f}s. {len(ids) - len(failed)}/{len(ids)} collections extracted.")
    if failed:
        log.info(f"Failed IDs: {', '.join(failed)}")


if __name__ == "__main__":
    main()