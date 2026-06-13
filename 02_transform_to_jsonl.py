"""
02_transform_to_jsonl.py

Stage 2 of the pipeline: transform extracted Arclight JSON (from
01_findingaid_json_extractor_arclight.py) into clean, flat JSONL ready
for chunking and embedding.

Handles both extractor output formats: the original verbose JSON-API shape
(components with 'attributes' wrappers and an 'enriched' sub-dict) and the
slim flattened shape.

Per collection, writes one .jsonl file. Each line is one record:

  - record_type "collection": one record per prose section (bioghist,
    scopecontent) of the collection-level description; falls back to the
    abstract if neither section exists.
  - record_type "component": one record per component, with rebuilt
    breadcrumb, cleaned text, containers, date, and gap flags.

Cleaning applied: HTML tags stripped, entities unescaped, whitespace
collapsed, "true"/"false" strings coerced to booleans, breadcrumbs rebuilt
from parent_ids (fixes the ancestor-prefix bug in earlier extractions).

Usage:
    python 02_transform_to_jsonl.py --in json-extraction --out jsonl-output

Options:
    --in DIR    Directory of extracted *.json files (default: json-extraction/)
    --out DIR   Output directory for *.jsonl files (default: jsonl-output/)
"""

import argparse
import html
import json
import logging
import re
import sys
import time
from pathlib import Path

BASE_URL = "https://findingaids.lib.umich.edu"

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


# ── Cleaning helpers ──────────────────────────────────────────────────────────

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def clean_text(value) -> str:
    """Strip HTML tags, unescape entities, collapse whitespace.

    Accepts a string or a list of strings (Solr multivalued fields);
    list items are joined with double newlines as paragraphs.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        return "\n\n".join(filter(None, (clean_text(v) for v in value)))
    text = _TAG_RE.sub(" ", str(value))
    text = html.unescape(text)
    return _WS_RE.sub(" ", text).strip()


def first(value):
    """First element of a Solr multivalued field, or the value itself."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _attr_val(field):
    """Extract .attributes.value from a JSON-API attribute object, or return as-is."""
    if isinstance(field, dict):
        return field.get("attributes", {}).get("value")
    return field


# ── Input normalization (fat JSON-API shape → slim) ───────────────────────────

def normalize_component(comp: dict) -> dict:
    """Return a flat component dict regardless of input format."""
    if "attributes" not in comp:
        return comp  # already slim

    slim = {"id": comp["id"]}
    for key, field in comp["attributes"].items():
        val = _attr_val(field)
        if key.endswith("?"):
            slim[key.rstrip("?")] = val == "true" if isinstance(val, str) else bool(val)
        else:
            slim[key] = val
    enriched = dict(comp.get("enriched", {}))
    # redundant with parent_ids / level in the slim shape
    enriched.pop("parent_ssim", None)
    enriched.pop("level_ssm", None)
    slim.update(enriched)
    return slim


# ── Transformation ─────────────────────────────────────────────────────────────

def normalize_parent_ids(parent_ids, ead_id: str) -> list[str]:
    """Prefix bare ancestor ids (aspace_xxx) with the collection's ead_id."""
    if not parent_ids:
        return []
    if isinstance(parent_ids, str):
        parent_ids = [parent_ids]
    return [
        pid if pid == ead_id or pid.startswith(f"{ead_id}_") else f"{ead_id}_{pid}"
        for pid in parent_ids
    ]


def build_breadcrumb(comp: dict, lookup: dict, ead_id: str) -> str:
    """Assemble 'Collection: X → Series: Y → File: Z' from normalized ancestry."""
    parts = []
    for pid in normalize_parent_ids(comp.get("parent_ids"), ead_id):
        if pid in lookup:
            level, title = lookup[pid]
            parts.append(f"{level}: {title}")
        else:
            log.warning(f"Breadcrumb ancestor not found for {comp['id']}: {pid}")
    level, title = lookup[comp["id"]]
    parts.append(f"{level}: {title}")
    return " → ".join(parts)


def collection_records(coll: dict, ead_id: str) -> list[dict]:
    """One record per prose section of the collection-level description."""
    title = clean_text(first(coll.get("title_ssm")))
    date = first(coll.get("normalized_date_ssm")) or first(coll.get("collection_date_inclusive_ssm"))
    records = []

    sections = [
        ("bioghist", coll.get("bioghist_tesim")),
        ("scopecontent", coll.get("scopecontent_tesim")),
    ]
    available = [(name, val) for name, val in sections if val]
    if not available:
        available = [("abstract", coll.get("abstract_tesim"))]

    for name, val in available:
        text = clean_text(val)
        if not text:
            continue
        records.append({
            "id": f"{ead_id}#{name}",
            "ead_id": ead_id,
            "record_type": "collection",
            "section": name,
            "level": "Collection",
            "title": title,
            "breadcrumb": f"Collection: {title}",
            "text": text,
            "parent_ids": [],
            "containers": [],
            "date": date,
            "online_content": None,
            "arclight_url": f"{BASE_URL}/catalog/{ead_id}",
            "flags": [],
        })
    return records


def component_record(comp: dict, lookup: dict, ead_id: str) -> dict:
    """Flat, cleaned record for one component."""
    title = clean_text(comp.get("title") or comp.get("normalized_title"))
    description = clean_text(comp.get("short_description"))
    scope = clean_text(comp.get("scopecontent_tesim"))
    containers = comp.get("containers_ssim") or []
    date = (first(comp.get("normalized_date_ssm"))
            or first(comp.get("unitdate_inclusive_ssm"))
            or first(comp.get("unitdate_ssm")))

    text = ". ".join(filter(None, [title, description, scope]))

    flags = []
    if not description and not scope:
        flags.append("no_description")
    if not containers:
        flags.append("no_containers")
    if not date:
        flags.append("no_date")

    return {
        "id": comp["id"],
        "ead_id": ead_id,
        "record_type": "component",
        "level": comp.get("level") or "Component",
        "title": title,
        "breadcrumb": build_breadcrumb(comp, lookup, ead_id),
        "text": text,
        "parent_ids": normalize_parent_ids(comp.get("parent_ids"), ead_id),
        "containers": containers,
        "date": date,
        "online_content": comp.get("online_content"),
        "arclight_url": f"{BASE_URL}/catalog/{comp['id']}",
        "flags": flags,
    }


def transform_file(path: Path, out_dir: Path) -> dict:
    """Transform one extracted collection JSON into a JSONL file.

    Returns summary stats for reporting.
    """
    data = json.loads(path.read_text())
    ead_id = data["ead_id"]
    coll = data["collection"]
    components = [normalize_component(c) for c in data.get("components", [])]

    # Lookup for breadcrumb assembly: id -> (level, cleaned title)
    lookup = {ead_id: ("Collection", clean_text(first(coll.get("title_ssm"))))}
    for comp in components:
        level = comp.get("level") or "Component"
        title = clean_text(comp.get("title") or comp.get("normalized_title")) or comp["id"]
        lookup[comp["id"]] = (level, title)

    records = collection_records(coll, ead_id)
    n_collection = len(records)
    flag_counts = {}
    for comp in components:
        rec = component_record(comp, lookup, ead_id)
        for f in rec["flags"]:
            flag_counts[f] = flag_counts.get(f, 0) + 1
        records.append(rec)

    out_path = out_dir / f"{ead_id}.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    log.info(f"✓ {ead_id}: {n_collection} collection records, "
             f"{len(components)} components, flags: {flag_counts or 'none'}")
    log.info(f"  Saved → {out_path}")
    return {"records": len(records), "components": len(components), "flags": flag_counts}


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Transform extracted Arclight JSON into clean JSONL."
    )
    parser.add_argument(
        "--in", dest="in_dir", default="json-extraction",
        help="Directory of extracted *.json files (default: json-extraction/)"
    )
    parser.add_argument(
        "--out", dest="out_dir", default="jsonl-output",
        help="Output directory for *.jsonl files (default: jsonl-output/)"
    )
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(out_dir / "transform.log")

    files = sorted(in_dir.glob("*.json"))
    if not files:
        log.error(f"No .json files found in {in_dir}")
        sys.exit(1)

    log.info(f"Transforming {len(files)} files from {in_dir} → {out_dir}")
    start = time.monotonic()

    totals = {"records": 0, "components": 0}
    failed = []
    for path in files:
        try:
            stats = transform_file(path, out_dir)
            totals["records"] += stats["records"]
            totals["components"] += stats["components"]
        except Exception as e:
            log.error(f"✗ Failed: {path.name} — {e}")
            failed.append(path.name)

    elapsed = time.monotonic() - start
    log.info("═" * 60)
    log.info(f"Done in {elapsed:.1f}s. {len(files) - len(failed)}/{len(files)} files transformed, "
             f"{totals['records']} records ({totals['components']} components).")
    if failed:
        log.info(f"Failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
