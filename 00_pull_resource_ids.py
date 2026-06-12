"""
00_pull_resource_ids.py

Fetch finding aid EAD IDs from an Arclight instance by repository,
and write them to a file.

Usage:
    python 00_pull_resource_ids.py --list
    python 00_pull_resource_ids.py --repo bentley
    python 00_pull_resource_ids.py --repo bentley --output data/ids/bentley.txt
    python 00_pull_resource_ids.py --repo all --output data/ids/all.csv
    python 00_pull_resource_ids.py --file resource-ids/resource_ids.txt --output data/ids/reformatted.csv
"""

import argparse
import csv
import sys
import time
from pathlib import Path

import requests

# ── Configuration ─────────────────────────────────────────────────────────────

ARCLIGHT_BASE = "https://findingaids.lib.umich.edu"
RATE_LIMIT    = 1.0
PER_PAGE      = 100

HEADERS = {
    "User-Agent": "arclight-extractor/1.0 (research data pipeline; contact: jajohnst@umich.edu)",
    "Accept":     "application/json",
}

REPOSITORIES = {
    "bentley":  "University of Michigan Bentley Historical Library",
    "scrc":     "University of Michigan Special Collections Research Center",
    "clements": "University of Michigan William L. Clements Library",
    "clarke":   "Central Michigan University Clarke Historical Library",
    "michigan": "Archives of Michigan",
    "genesee":  "University of Michigan Genesee Historical Collections Center",
    "mlibrary": "MLibrary Finding Aids",
    "asia":     "University of Michigan Asia Library",
    "vrc":      "University of Michigan History of Art Visual Resources Collection",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def list_repositories() -> None:
    print("\nAvailable repository aliases:\n")
    col = max(len(a) for a in REPOSITORIES) + 2
    for alias, name in REPOSITORIES.items():
        print(f"  {alias:<{col}} {name}")
    print()


def get_ead_ids(repository: str) -> list[tuple[str, str]]:
    """
    Fetch all collection-level EAD IDs for a repository, paginated.
    Returns a list of (alias, ead_id) tuples.
    """
    repo_name = REPOSITORIES.get(repository.lower(), repository)
    alias     = repository.lower()
    print(f"\nFetching: {repo_name}")

    results = []
    page    = 1

    while True:
        time.sleep(RATE_LIMIT)
        params = {
            "f[repository_sim][]": repo_name,
            "f[level_sim][]":      "Collection",
            "per_page":            PER_PAGE,
            "page":                page,
        }
        response = requests.get(
            f"{ARCLIGHT_BASE}/catalog.json",
            params=params,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        for record in data.get("data", []):
            results.append((alias, record["id"]))

        pages       = data.get("meta", {}).get("pages", {})
        total_pages = pages.get("total_pages", 1)
        total_count = pages.get("total_count", 0)

        print(f"  Page {page}/{total_pages} — "
              f"{len(results)}/{total_count} IDs collected")

        if page >= total_pages:
            break
        page += 1

    print(f"  → {len(results)} IDs for '{alias}'")
    return results


def get_all_ead_ids() -> list[tuple[str, str]]:
    """Fetch EAD IDs across all known repositories."""
    results = []
    for alias in REPOSITORIES:
        results.extend(get_ead_ids(alias))
    print(f"\nTotal: {len(results)} IDs across {len(REPOSITORIES)} repositories.")
    return results


def load_from_file(path: Path) -> list[tuple[str, str]]:
    """Load EAD IDs from an existing .txt or .csv file as (alias, ead_id) tuples."""
    rows = []
    if path.suffix.lower() == ".csv":
        with path.open() as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if not row:
                    continue
                if i == 0 and row[0].strip().lower() in ("repository", "ead_id", "id"):
                    continue  # skip header
                alias  = row[0].strip() if len(row) > 1 else ""
                ead_id = (row[1] if len(row) > 1 else row[0]).strip()
                if ead_id:
                    rows.append((alias, ead_id))
    else:
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                rows.append(("", line))
    return rows


# ── Output ─────────────────────────────────────────────────────────────────────

def write(rows: list[tuple[str, str]], path: Path) -> None:
    """Write to .txt (IDs only) or .csv (repository + ID) based on file extension."""
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix.lower() == ".csv":
        with path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["repository", "ead_id"])
            writer.writerows(rows)
    else:
        path.write_text("\n".join(eid for _, eid in rows) + "\n")

    print(f"\nSaved {len(rows)} IDs → {path}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="00_pull_resource_ids",
        description="Fetch finding aid EAD IDs from Arclight by repository.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python 00_pull_resource_ids.py --list
  python 00_pull_resource_ids.py --repo bentley
  python 00_pull_resource_ids.py --repo bentley --output data/ids/bentley.txt
  python 00_pull_resource_ids.py --repo all --output data/ids/all.csv
  python 00_pull_resource_ids.py --file resource-ids/old_ids.txt --output data/ids/reformatted.csv
        """,
    )

    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--list",
        action="store_true",
        help="List available repository aliases and exit.",
    )
    source.add_argument(
        "--repo",
        metavar="ALIAS",
        help="Repository alias to fetch (e.g. 'bentley'), or 'all'.",
    )
    source.add_argument(
        "--file",
        metavar="PATH",
        help="Load from an existing .txt or .csv file instead of fetching.",
    )

    parser.add_argument(
        "--output",
        metavar="PATH",
        help=(
            "Output file path including filename and extension "
            "(e.g. data/ids/bentley.txt or data/ids/all.csv). "
            "Extension determines format: .txt = one ID per line, "
            ".csv = repository + ID columns. "
            "Defaults to <alias>.txt in the current directory."
        ),
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=RATE_LIMIT,
        metavar="SECONDS",
        help=f"Seconds between requests (default: {RATE_LIMIT}).",
    )

    return parser


def default_output(label: str, rows: list[tuple[str, str]]) -> Path:
    """Generate a default output path when --output is not specified."""
    ext = ".csv" if label == "all" else ".txt"
    return Path(f"{label}{ext}")


def main() -> None:
    global RATE_LIMIT

    parser = build_parser()
    args   = parser.parse_args()

    RATE_LIMIT = args.rate_limit

    if args.list:
        list_repositories()
        sys.exit(0)

    if args.file:
        path = Path(args.file)
        if not path.exists():
            print(f"Error: file not found: {path}", file=sys.stderr)
            sys.exit(1)
        print(f"Loading IDs from: {path}")
        rows = load_from_file(path)
        print(f"  {len(rows)} IDs loaded.")
        out = Path(args.output) if args.output else default_output(path.stem, rows)
        write(rows, out)
        sys.exit(0)

    if args.repo:
        rows  = get_all_ead_ids() if args.repo.lower() == "all" else get_ead_ids(args.repo)
        label = args.repo.lower()
        out   = Path(args.output) if args.output else default_output(label, rows)
        write(rows, out)
        sys.exit(0)

    parser.print_help()


if __name__ == "__main__":
    main()