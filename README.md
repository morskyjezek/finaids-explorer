# Finding Aids Explorer Dev Repo

This repo contains basic content regarding
initial steps in developing a data pipeline,
vector database, and interface for a natural-language
queryable system for interacting with archival finding aids.
Part of the goal is to see how small language, lightweight,
and open source models might provide an interaction
interface for archival finding aids.

## Pipeline

As of June 2026, the pipeline only includes scripts for querying and extracting finding aid information that is stored in ASpace but queryable using an ArcLight interface.

- `00_pull_resource_ids.py` — fetch finding aid EAD IDs by repository
- `01_findingaid_json_extractor_arclight.py` — extract collection metadata and component records per ID
- `02_transform_to_jsonl.py` — clean and flatten into JSONL records

## Requirements

See `requirements.txt` for a list of current requirements. For the extraction and transformation steps,
the most important element is the `requests` library,
which enables the calls to query an API.

## Status

This is an in-process project, as of June 2026.
The code and pipeline in this repo were developed with significant input from claude code.

Any code or data in this repository is for demonstration
purposes only. While the code is made available
under a GNU public licencse, there are no guarantees
this code will work with other workflows or data
cleaning and preparation steps.
