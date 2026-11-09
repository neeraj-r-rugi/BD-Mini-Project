"""Node 4 - FastAPI search service using BM25 and PageRank."""

import math
import re
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config_loader import CFG, path
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

SERVING = path("SERVING_DIR")
K1, B = float(CFG["BM25_K1"]), float(CFG["BM25_B"])
W_BM25, W_PR = float(CFG["W_BM25"]), float(CFG["W_PAGERANK"])
TOKEN_RE = re.compile(CFG["TOKEN_REGEX"])
DEFAULT_K = int(CFG["TOP_K"])

app = FastAPI(title="Mini Search Engine")
_lock = threading.Lock()
STATE: "IndexState | None" = None


@dataclass(frozen=True)
class IndexState:
    version: int
    n_docs: int
    avgdl: float
    doc_len: np.ndarray
    pagerank: np.ndarray
    max_pr: float
    doc_ids: list[str]
    urls: list[str]
    titles: list[str]
    inv: dict
    manifest: dict


def tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall(text.lower())


def load_version(version: int) -> IndexState:
    directory = SERVING / f"v{version}"
    for name in ("postings", "docs", "manifest"):
        if not (directory / name / "_SUCCESS").exists():
            raise ValueError(f"{name} incomplete")
    docs = pd.read_parquet(directory / "docs")
    postings = pd.read_parquet(directory / "postings")
    manifest_df = pd.read_parquet(directory / "manifest")
    if manifest_df.empty:
        raise ValueError("manifest is empty")
    manifest = manifest_df.iloc[0].to_dict()
    if len(docs) != int(manifest["n_docs"]) or len(postings) != int(
        manifest["n_postings"]
    ):
        raise ValueError("count mismatch")
    required_docs = {"doc_id", "url", "title", "doc_len", "pagerank"}
    required_postings = {"term", "doc_id", "tf"}
    if not required_docs.issubset(docs.columns) or not required_postings.issubset(
        postings.columns
    ):
        raise ValueError("export schema is incomplete")
    if docs["doc_id"].duplicated().any() or docs["url"].isna().any():
        raise ValueError("invalid document metadata")
    index = {doc_id: i for i, doc_id in enumerate(docs["doc_id"])}
    postings = postings.copy()
    postings["i"] = postings["doc_id"].map(index)
    if postings["i"].isna().any():
        raise ValueError("posting references an unknown document")
    inv = {
        term: (group["i"].to_numpy(dtype=int), group["tf"].to_numpy(dtype=float))
        for term, group in postings.groupby("term")
    }
    avgdl = float(docs["doc_len"].mean())
    max_pr = float(docs["pagerank"].max())
    if avgdl <= 0 or max_pr <= 0:
        raise ValueError("invalid document lengths or PageRank")
    return IndexState(
        version=version,
        n_docs=len(docs),
        avgdl=avgdl,
        doc_len=docs["doc_len"].to_numpy(dtype=float),
        pagerank=docs["pagerank"].to_numpy(dtype=float),
        max_pr=max_pr,
        doc_ids=docs["doc_id"].tolist(),
        urls=docs["url"].tolist(),
        titles=docs["title"].tolist(),
        inv=inv,
        manifest=manifest,
    )


def search(state: IndexState, q: str, k: int) -> list[dict]:
    scores: dict[int, float] = {}
    for term in set(tokenize(q)):
        if term not in state.inv:
            continue
        indices, tf = state.inv[term]
        df = len(indices)
        idf = math.log(1 + (state.n_docs - df + 0.5) / (df + 0.5))
        part = idf * tf * (K1 + 1) / (
            tf + K1 * (1 - B + B * state.doc_len[indices] / state.avgdl)
        )
        for index, value in zip(indices, part):
            scores[int(index)] = scores.get(int(index), 0.0) + float(value)
    if not scores:
        return []
    max_bm25 = max(scores.values())
    ranked = sorted(
        (
            W_BM25 * value / max_bm25
            + W_PR * state.pagerank[index] / state.max_pr,
            index,
            value,
        )
        for index, value in scores.items()
    )[::-1][: max(0, k)]
    return [
        {
            "rank": rank,
            "doc_id": state.doc_ids[index],
            "url": state.urls[index],
            "title": state.titles[index],
            "bm25": value,
            "pagerank": float(state.pagerank[index]),
            "score": float(score),
        }
        for rank, (score, index, value) in enumerate(ranked, 1)
    ]


def _current_version_from_symlink() -> int | None:
    current = SERVING / "current"
    if not current.is_symlink():
        return None
    match = re.fullmatch(r"v(\d+)", current.resolve().name)
    return int(match.group(1)) if match else None


@app.on_event("startup")
def startup():
    global STATE
    version = _current_version_from_symlink()
    if version is not None:
        try:
            STATE = load_version(version)
        except Exception as exc:
            print(f"failed to load version {version}: {exc}", file=sys.stderr)


@app.get("/search")
def search_endpoint(q: str = Query(..., min_length=1), k: int = DEFAULT_K):
    state = STATE
    if state is None:
        raise HTTPException(503, "index not loaded")
    return {"version": state.version, "query": q, "results": search(state, q, k)}


@app.get("/version")
def version_endpoint():
    state = STATE
    if state is None:
        raise HTTPException(503, "index not loaded")
    return {"version": state.version, "manifest": state.manifest}


@app.post("/reload")
def reload_endpoint(version: int | None = None):
    if version is None:
        versions = [
            int(match.group(1))
            for entry in SERVING.iterdir()
            if (match := re.fullmatch(r"v(\d+)", entry.name)) and entry.is_dir()
        ]
        if not versions:
            raise HTTPException(400, "no export versions found")
        version = max(versions)
    try:
        new_state = load_version(version)
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc
    global STATE
    with _lock:
        STATE = new_state
    return {"status": "ok", "version": version}


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")
