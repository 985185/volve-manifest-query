# src/volve_query/models.py

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import AliasChoices, BaseModel, Field


EntryType = Literal["file", "directory"]


class ManifestNode(BaseModel):
    """
    A raw node inside a manifest bucket list.

    We keep this permissive because upstream manifests may contain tags as:
    - "" (empty string)
    - "DOCS|WELL_TECH"
    - ["DOCS", "WELL_TECH"]
    """
    path: str
    name: str
    type: EntryType
    ext_norm: str = ""
    top_folder: str = ""
    tags: Union[str, List[str], None] = None
    foreign_ref_wells: List[str] = Field(default_factory=list)


class Manifest(BaseModel):
    """
    Normalized manifest model.

    Some older manifests use `well` instead of `well_id`.
    We accept either to avoid startup failures.
    """
    well_id: str = Field(validation_alias=AliasChoices("well_id", "well"))
    buckets: Dict[str, List[ManifestNode]] = Field(default_factory=dict)

    # Optional (not required for functionality, but nice to have if present)
    bucket_counts: Optional[Dict[str, int]] = None
    foreign_ref: Optional[List[str]] = None


class FileEntry(BaseModel):
    type: EntryType
    well_key: str
    well_id: str
    bucket: str
    path: str
    filename: str
    tags: List[str] = Field(default_factory=list)


class WellSummary(BaseModel):
    well_key: str
    well_id: str
    bucket_counts: Dict[str, int]
    total_files: int
    foreign_reference_count: int = 0


class HealthResponse(BaseModel):
    status: Literal["ok"]
    wells_loaded: int
    manifests_loaded: int
    source_dir: str


class WellsResponse(BaseModel):
    count: int
    wells: List[str]


class SummaryListResponse(BaseModel):
    total: int
    offset: int
    limit: int
    count: int
    wells: List[WellSummary]


class SearchResponse(BaseModel):
    query: str
    filters: Dict[str, Optional[str]]
    total: int
    offset: int
    limit: int
    results: List[FileEntry]