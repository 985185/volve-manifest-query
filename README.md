# Volve Manifest Query API

A FastAPI service to query Volve well manifests, including search, summary, and CSV export.

## Features

- Load well manifests from a `wells/` directory
- Global and per-well search
- Pagination and deduplication options
- CSV export of search results
- Bucket navigation and file browsing

## Endpoints

**Health**

GET /health


**Wells**

GET /wells
GET /wells/{well_key}/summary
GET /wells/{well_key}/buckets
GET /wells/{well_key}/buckets/{bucket_name}
GET /wells/{well_key}/manifest
GET /wells/{well_key}/foreign-references


**Search**

GET /search?q=<QUERY>&bucket=<BUCKET>&dedupe_key=path
GET /wells/{well_key}/search?q=<QUERY>
GET /search.csv?q=<QUERY>
GET /wells/{well_key}/search.csv?q=<QUERY>


## Running Locally

```bash
pip install -e .
volveq serve --reload
Examples
http://localhost:8000/search?q=geological_summary
http://localhost:8000/summary?limit=10
http://localhost:8000/search.csv?q=DDR_XML
