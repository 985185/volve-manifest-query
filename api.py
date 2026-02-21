# src/volve_query/api.py

from __future__ import annotations

import csv
import io
import os
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse

from .index import ManifestIndex, SearchHit
from .loader import load_manifests_from_wells_dir
from .models import (
    FileEntry,
    HealthResponse,
    SearchResponse,
    SummaryListResponse,
    WellSummary,
    WellsResponse,
)

ENV_WELLS_DIR = "VOLVE_WELLS_DIR"

# In-memory state
_INDEX: Optional[ManifestIndex] = None
_SOURCE_DIR: str = "wells"


def _get_index() -> ManifestIndex:
    if _INDEX is None:
        raise HTTPException(status_code=500, detail="Manifests not loaded")
    return _INDEX


def init_app(wells_dir: str) -> None:
    global _INDEX, _SOURCE_DIR
    _SOURCE_DIR = wells_dir
    manifests = load_manifests_from_wells_dir(wells_dir)
    _INDEX = ManifestIndex(manifests)


# Fix: use lifespan instead of deprecated @app.on_event("startup")
@asynccontextmanager
async def lifespan(app: FastAPI):
    wells_dir = os.getenv(ENV_WELLS_DIR, "wells")
    init_app(wells_dir)
    yield


app = FastAPI(
    title="Volve Manifest Query API",
    version="0.0.1",
    lifespan=lifespan,
)


def _hit_to_entry(h: SearchHit) -> FileEntry:
    return FileEntry(
        type=h.type,
        well_key=h.well_key,
        well_id=h.well_id,
        bucket=h.bucket,
        path=h.path,
        filename=h.filename,
        tags=h.tags,
    )


def _dedupe_results(results: List[FileEntry], key: str) -> List[FileEntry]:
    seen: set = set()
    out: List[FileEntry] = []
    for r in results:
        k = r.path if key == "path" else f"{r.well_key}::{r.filename}"
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def _as_csv_rows(entries: List[FileEntry]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["well_key", "well_id", "bucket", "filename", "path", "tags"])
    for e in entries:
        writer.writerow([e.well_key, e.well_id, e.bucket, e.filename, e.path, "|".join(e.tags)])
    return buf.getvalue()


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    count = len(_INDEX.manifests) if _INDEX else 0
    return HealthResponse(
        status="ok",
        wells_loaded=count,
        manifests_loaded=count,
        source_dir=_SOURCE_DIR,
    )


@app.get("/wells", response_model=WellsResponse)
def list_wells() -> WellsResponse:
    idx = _get_index()
    wells = idx.wells()
    return WellsResponse(count=len(wells), wells=wells)


@app.get("/wells/{well_key}/manifest", response_model=dict)
def get_manifest(well_key: str) -> dict:
    idx = _get_index()
    if well_key not in idx.manifests:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    return idx.manifest_raw(well_key)


@app.get("/wells/{well_key}/summary", response_model=WellSummary)
def well_summary(well_key: str) -> WellSummary:
    idx = _get_index()
    if well_key not in idx.manifests:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    s = idx.summary(well_key)
    return WellSummary(**s)


@app.get("/summary", response_model=SummaryListResponse)
def summary(
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> SummaryListResponse:
    idx = _get_index()
    wells = idx.wells()
    total = len(wells)
    page_keys = wells[offset : offset + limit]
    summaries = [WellSummary(**idx.summary(wk)) for wk in page_keys]
    return SummaryListResponse(
        total=total,
        offset=offset,
        limit=limit,
        count=len(summaries),
        wells=summaries,
    )


@app.get("/wells/{well_key}/buckets", response_model=dict)
def well_buckets(well_key: str) -> dict:
    idx = _get_index()
    if well_key not in idx.manifests:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    return {"well_key": well_key, "buckets": idx.buckets(well_key)}


@app.get("/wells/{well_key}/buckets/{bucket_name}", response_model=dict)
def bucket_files(
    well_key: str,
    bucket_name: str,
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> dict:
    idx = _get_index()
    if well_key not in idx.manifests:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    hits = idx.bucket_files(well_key, bucket_name)
    if hits is None:
        raise HTTPException(status_code=404, detail="Unknown bucket for this well")
    total = len(hits)
    page = [_hit_to_entry(h).__dict__ for h in hits[offset : offset + limit]]
    return {
        "well_key": well_key,
        "bucket": bucket_name,
        "total": total,
        "offset": offset,
        "limit": limit,
        "files": page,
    }


@app.get("/wells/{well_key}/foreign-references", response_model=dict)
def foreign_references(well_key: str) -> dict:
    idx = _get_index()
    if well_key not in idx.manifests:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    refs = idx.foreign_refs(well_key)
    return {"well_key": well_key, "count": len(refs), "references": refs}


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1),
    well_key: Optional[str] = Query(None),
    bucket: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    include_dirs: bool = Query(False, description="Include directory entries in results"),
    dedupe: bool = Query(True, description="Dedupe results (recommended)"),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> SearchResponse:
    idx = _get_index()
    # Use full result set from index then apply dedupe + pagination here
    total_hits, hits = idx.search(q=q, well_key=well_key, bucket=bucket, limit=999_999, offset=0)
    results = [_hit_to_entry(h) for h in hits if include_dirs or h.type != "directory"]
    if dedupe:
        results = _dedupe_results(results, key=dedupe_key)
    total = len(results)
    page = results[offset : offset + limit]
    return SearchResponse(
        query=q,
        filters={"well_key": well_key, "bucket": bucket},
        total=total,
        offset=offset,
        limit=limit,
        results=page,
    )


@app.get("/wells/{well_key}/search", response_model=SearchResponse)
def search_in_well(
    well_key: str,
    q: str = Query(..., min_length=1),
    bucket: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    include_dirs: bool = Query(False),
    dedupe: bool = Query(True),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> SearchResponse:
    idx = _get_index()
    if well_key not in idx.manifests:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    _, hits = idx.search(q=q, well_key=well_key, bucket=bucket, limit=999_999, offset=0)
    results = [_hit_to_entry(h) for h in hits if include_dirs or h.type != "directory"]
    if dedupe:
        results = _dedupe_results(results, key=dedupe_key)
    total = len(results)
    page = results[offset : offset + limit]
    return SearchResponse(
        query=q,
        filters={"well_key": well_key, "bucket": bucket},
        total=total,
        offset=offset,
        limit=limit,
        results=page,
    )


@app.get("/search.csv", response_class=PlainTextResponse)
def search_csv(
    q: str = Query(..., min_length=1),
    well_key: Optional[str] = Query(None),
    bucket: Optional[str] = Query(None),
    include_dirs: bool = Query(False),
    dedupe: bool = Query(True),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> PlainTextResponse:
    idx = _get_index()
    _, hits = idx.search(q=q, well_key=well_key, bucket=bucket, limit=999_999, offset=0)
    results = [_hit_to_entry(h) for h in hits if include_dirs or h.type != "directory"]
    if dedupe:
        results = _dedupe_results(results, key=dedupe_key)
    return PlainTextResponse(content=_as_csv_rows(results), media_type="text/csv")


@app.get("/wells/{well_key}/search.csv", response_class=PlainTextResponse)
def search_in_well_csv(
    well_key: str,
    q: str = Query(..., min_length=1),
    bucket: Optional[str] = Query(None),
    include_dirs: bool = Query(False),
    dedupe: bool = Query(True),
    dedupe_key: str = Query("path", pattern="^(path|filename)$"),
) -> PlainTextResponse:
    idx = _get_index()
    if well_key not in idx.manifests:
        raise HTTPException(status_code=404, detail="Unknown well_key")
    _, hits = idx.search(q=q, well_key=well_key, bucket=bucket, limit=999_999, offset=0)
    results = [_hit_to_entry(h) for h in hits if include_dirs or h.type != "directory"]
    if dedupe:
        results = _dedupe_results(results, key=dedupe_key)
    return PlainTextResponse(content=_as_csv_rows(results), media_type="text/csv")
