# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Backend (FastAPI)
```bash
cd backend
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload  # http://localhost:8000/docs
```

### Frontend (Next.js 14)
```bash
cd frontend
npm install
npm run dev    # http://localhost:3000
npm run build
npm run lint
```

### Full stack (Docker)
```bash
OPENAI_API_KEY=<key> docker-compose up --build
```

No test infrastructure exists in this project.

## Architecture

Three-step pipeline: upload CSV/Excel → profile & detect issues → pick cleaning strategies → generate standalone Python script.

```
POST /analyze/   → DataProfiler → issues + column metadata
POST /suggest/batch → rule-based strategies per issue
POST /generate/  → validated, downloadable .py script
```

### Backend (`backend/app/`)

**`routers/analyze.py`** — Accepts CSV/Excel (max 100MB), delegates to `DataProfiler`, returns issues + column info.

**`services/profiler.py`** — Core analysis engine. Auto-detects encoding (UTF-8/Latin-1/cp1252) and delimiter (comma/semicolon). Normalizes column names to snake_case. Classifies each column's semantic type: `numeric`, `numeric-mixed`, `categorical`, `id`, `datetime`, `datetime-mixed`, `text`. Detects 25+ null-string patterns beyond NaN. Issue severity: `critical` (>30% missing) / `high` / `medium` / `low`.

**`routers/suggest.py`** — Rule-based strategies per issue type. Each strategy includes `name`, `pros`, `cons`, and `code_preview` (the actual code snippet used for generation). The `recommended` index points to the best strategy.

**`routers/generate.py`** — Assembles a complete, standalone `.py` script from the selected strategies' `code_preview` fields. Validates syntax with Python's `ast` module before returning. Auto-corrects deprecated pandas methods (e.g. `fillna(method='ffill')` → `ffill()`). Generated scripts include their own `detect_encoding`/`detect_delimiter`/`load_data` helpers so they run without this app.

**`config.py`** — `OPENAI_API_KEY` is plumbed but the suggestion router is currently rule-based only; the key is reserved for future LLM-powered suggestions.

### Frontend (`frontend/`)

Next.js 14 App Router with TypeScript and Tailwind CSS. Single page at `app/page.tsx`.

**Component flow:**
1. `FileUpload` — drag-and-drop or click to upload; calls `POST /analyze/`
2. `AnalysisResult` — on mount calls `POST /suggest/batch` to pre-load strategies and auto-select the recommended one per issue. Clicking an issue opens `SuggestionModal` to override the strategy.
3. `SuggestionModal` — shows pros/cons and code preview per strategy; user selects one.
4. `CodeExport` — displays the assembled pipeline steps, triggers `POST /generate/`, then shows/downloads the generated script.

All API calls are in `lib/api.ts`. Two axios instances: `apiFormData` (no Content-Type header, lets axios set the multipart boundary) for file upload; `api` (JSON) for everything else.

### Environment
- `OPENAI_API_KEY` — backend reads from env; not currently used
- `NEXT_PUBLIC_API_URL` — frontend API base URL, defaults to `http://localhost:8000`
- CORS allows `localhost:3000` and `localhost:5173`
- Backend settings via `app/config.py` (pydantic-settings); reads `.env` if present
