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

### Tests
```bash
cd backend
pip install pandas numpy openpyxl fastapi "pydantic>=2" pydantic-settings python-multipart httpx openai pytest
pytest tests/ -q   # 151 tests : advisor, quality, agent_cleaner, contract, reparer, corpus, preparation, bout-en-bout
```
CI : `.github/workflows/tests.yml` (push/PR). La boucle agentique tourne 100 % offline (conseiller fail-open → rule-based).

## Architecture

Three-step pipeline: upload CSV/Excel → profile & detect issues → pick cleaning strategies → generate standalone Python script.

```
POST /analyze/   → DataProfiler → issues + column metadata
POST /suggest/batch → rule-based strategies per issue (+ LLM advisor picks/justifies)
POST /generate/  → validated, downloadable .py script
POST /auto-clean/ → one-pass autopilot (analyse → pick → script)
POST /agent-clean/ → AGENTIC LOOP: measure quality → clean → re-profile → iterate,
                     keeping the best version (services/agent_cleaner.py + quality.py)
POST /contract-clean/propose → the data contract this file would have to satisfy,
                     given a declared USAGE; changes nothing (contract_infer.py)
POST /contract-clean/  → CONTRACT PASS: repair toward the contract, verify it
                     independently, deliver data + typed Parquet + what is
                     guaranteed and what is not (agent_contrat.py + reparer.py)
GET  /contract-clean/usages → the four usages and their labels
POST /prepare/        → the PREPARATION CHAIN: a standalone sklearn Pipeline,
                     fitted after the split, never on the whole file
                     (services/preparation.py)
```

### Contract pass vs agentic loop — read this before touching either

`/agent-clean` optimises **its own profiler's issue list**: `quality.py` starts
at 100 and subtracts a penalty per detected issue. So the loop maximises "my
profiler no longer complains", which is not the same as "the data is usable".
Measured on a 430-row CSV it reported **100/100** on output where:

- `inconnu` had been imputed into 247 of 400 rows (it was the column's mode, and
  `inconnu` is **not** in `DataProfiler.MISSING_PATTERNS`, which has `Unknown`);
- `Rabat` and `rabat` were still distinct (no case check exists in the profiler);
- a third of a numeric column sat on its median as a point mass;
- every column came out as text.

`/contract-clean/` fixes the objective rather than adding more rules. Three
properties make it work, and all three are load-bearing:

1. **`contract.py` never consults the profiler.** Every check is a measurement on
   the DataFrame. That is what lets a report contradict the cleaner. Keep it that
   way — inference lives in `contract_infer.py` precisely so the dependency only
   goes one way.
2. **A blocking violation caps the score** at `PLAFOND_BLOQUANT` (55). Without the
   cap, one fatal defect drowns under a majority of passing checks, which is
   exactly how 100/100 happened.
3. **Information loss is in the objective.** `lignes_perdues_max` and
   `imputation_max` make over-filling a *violation*, so the repair **refuses**
   rather than fabricating. `reparer.Journal.refus` is the deliverable: it says
   which guarantees the data cannot carry, and why.

Two traps that cost real debugging time, both now covered by tests:

- **Never inherit the profiler's blind spot.** `contract_infer` re-measures the
  void rate (`taux_vide_reel`) and re-infers the type (`type_apres_nettoyage`)
  on the marker-cleaned column. Without this, `age` was declared `string` with a
  30% null ceiling when it is an integer column 62% empty.
- **Measure rates on the post-dedup basis, not the raw file.** Dedup changes the
  denominator: a ceiling computed before the repair was violated by the repair
  itself (49.1% before, 51.0% after). `inferer` builds `base` first for this.

### What real files broke, and what it cost to find out

Everything above was validated on one synthetic 430-row CSV. Running the pass
over seven real files (Telco Churn, Boston Housing, Online Retail, the Bias
Bounty tables) found two defects a synthetic probe cannot produce, both now
covered by tests:

- **A boolean column crashed the profiler.** `pd.api.types.is_numeric_dtype`
  returns True for `bool`, so boolean columns went down the numeric branch into
  the 3xIQR outlier check, where numpy refuses to subtract two booleans. A file
  with 8 boolean columns out of 17 took the whole analysis down. `bool` is now
  classified `categorical`, which is what it is.
- **`numeric-mixed` was taken literally.** The profiler calls a column
  `numeric-mixed` when 70% of a sample parses as a number. On `StockCode` —
  2 684 numeric references and 424 codes like `15044B` — the contract declared
  `integer`, and conversion emptied 424 rows: the contract was violated by its
  own repair. `contract_infer.type_tenable` now declares a numeric, temporal or
  boolean type **only if the column holds it entirely**, and falls back to
  `string` with examples otherwise. *Mixed* does not mean *86% numeric*; it
  means the column holds codes, and a code is not a quantity.

### The preparation chain — `services/preparation.py`

**Read this before adding any scaling, encoding or normalisation anywhere
else.** This module exists because the ban on them was right and the gap was
real at the same time.

The ban: fitting a transformer on the whole file leaks test information into
training. A `StandardScaler` fitted before the split knows the mean of rows it
is meant never to have seen, and every evaluation afterwards is optimistic by
an amount nobody can measure. That reason has not gone away and must not be
traded against convenience.

The resolution: **do not transform the file, emit the chain.** `/prepare/`
returns a standalone script where `train_test_split` comes before `fit` and no
parameter allows the reverse — a parameter that permitted the leak is a
parameter somebody would eventually pass.

Three things are load-bearing:

1. **The type vocabulary is imported from `contract.TYPES`, not copied.** A
   duplicated list diverges, and a type with no role here would fall silently
   into the categorical branch and be one-hot encoded instead of excluded. The
   module asserts the correspondence at import and `test_preparation.py` pins
   it.
2. **The leak test executes the generated script** and compares the mean the
   `StandardScaler` actually learned against the training mean and the
   whole-file mean. An assertion on the script's *text* would pass over code
   that leaks. Checked by introducing the leak deliberately — note that
   `fit(X)` followed by `fit(X_train)` re-fits and erases it, so a sabotage
   must replace the fit, not precede it.
3. **Every exclusion carries its reason**, and the generated script writes
   them in its header. A column that disappears without explanation is a
   column somebody will believe was used. Key, constant, high-cardinality and
   temporal columns are dropped by rule, each with its count.

`MODALITES_MAX = 30` and `LIGNES_MIN = 50` are choices, not laws: declared at
module level, overridable, and written into the generated script so they can
be argued with.

### The pathology corpus — `tests/corpus.py`

Twenty files, **one pathology each**, with a stated expectation and a stated
stake. Real files find what you did not think of; the corpus finds what you did,
and tells you *which* case broke rather than handing you a tangle. `test_corpus.py`
runs them one by one so a failure names itself.

What it found on its first run, all now locked:

- **Leading zeros were being eaten.** `01000` became `1000`, and no type check
  could see it because the column held its declared type perfectly. The cause
  was at load: pandas converts before anything else runs. The contract pass now
  reads everything as text (`analyze_file(..., texte_brut=True)`) and decides
  types itself — the third time in this module that refusing to inherit the
  loader's verdict was the fix. A real file proves it: `SampleSubmission.csv`
  carries Arizona GEOIDs (`04001944902`) that previously became `4001944902`,
  which is exactly what the Bias Bounty brief warns about.
- **A header-only file crashed with a 500.** It now returns 400 and says a file
  with no data rows is normal before a first load and an incident after, and
  that nothing in the file distinguishes the two.
- **Accounting negatives `(1200)` went unflagged.** Stripping the parentheses
  turns a debit into a credit. The inference counts and names them and leaves
  the column as text; converting would mean deciding the file follows that
  convention, and `(A12)` looks the same.

**Writing a corpus case badly is easy.** The GEOID case first used FIPS 40
(Oklahoma), which has no leading zero, so it passed while testing nothing.
A case that cannot fail is worse than no case: it reports safety.

### The public corpus — `corpus-public/`

Five real datasets, downloaded rather than written, and **not versioned** (3.5 MB,
differing licences; the URLs are in `corpus-public/README.md` and re-fetching
takes thirty seconds). `tests/test_corpus_public.py` skips cleanly when the
directory is empty.

They are **data, not code**: they live outside `backend/`, nothing is ever
executed from that directory, and the scripts that read them take paths as
arguments. An interpreter started inside a data directory imports modules from
it, and a file named `json.py` dropped there would run on the first `import json`.

What they caught that the handwritten corpus did not:

- **`?` is missing from `DataProfiler.MISSING_PATTERNS`**, which has `???`. UCI
  Adult uses it on 5.6% of three columns — 1 700 voids that would have passed
  for modalities. The wide re-measure catches it, score 0 to 100.
- **Titanic vindicates the outlier default.** The pass flags `fare` = £512.33
  (three times) and `sibsp` = 8 as outside the plausible interval. Both are
  **real**: the Cardeza party held the most expensive tickets on the ship, and
  the Sage family travelled eleven. A tool that clipped by default would destroy
  the most interesting rows in the most-taught dataset in the field, silently,
  while reporting success. This is the argument for `politique_aberrantes =
  "signaler"`, and it is worth more than the reasoning that produced it.

### Outliers: measured always, declared on request, never silently clipped

The taxonomy of preprocessing lists "smoothing noisy data" under cleaning, and
the pass deliberately does **not** do it by default. Three positions, and the
default is the only one that fabricates nothing:

- `contract_infer.plage_plausible` computes a robust interval (IQR x3, the same
  factor `DataProfiler` uses, so the two never give different counts for one
  column) and the inference **always reports** how many values fall outside it,
  with examples, in `arbitrages`.
- The range is **declared** in the contract only with `proposer_plages=True`.
  Declaring it by default would make the contract unsatisfiable by the file it
  was inferred from — the same self-contradiction as the null-ceiling bug.
- `reparer.traiter_les_aberrantes` honours `ContratColonne.politique_aberrantes`:
  `"signaler"` (default) touches nothing and records a refusal; `"ecreter"` and
  `"vider"` are opt-in and obey the **imputation budget**, because clipping 300
  incomes to the bound invents 300 identical incomes that never existed.

The reason for the default is not timidity: an extreme value is sometimes a typo
and sometimes the most important row in the file — a fraud, an outage, an edge
case — and the column alone cannot tell them apart.

**`pd.api.types.is_numeric_dtype` returns True for `bool`.** This trap has now
closed three times in this project: the profiler's outlier quantiles, then
`plage_plausible`'s interquartile subtraction, then the fix for the second
promoting flag columns to `integer`. It is cut at the root in
`contract._numerique`, which casts bool to float, with guards in
`plage_plausible` and `type_apres_nettoyage`. Any new code that subtracts,
quantiles or clips a column must assume a bool can arrive.

**What the pass still does not do**, and mostly on purpose: the contract pass
itself performs no normalisation, encoding or scaling — that is `/prepare/`'s
job, and it does it by emitting a `Pipeline` fitted on the training split
rather than by touching the file. No multi-source integration and no reshaping; the
contract's key and uniqueness checks are the natural basis for a future
"are these two files joinable" command.

### Performance, measured

**1.8 MB/s for the whole pass** — 500 000 rows / 21 MB in 11.4 s (median of
five runs, sd 0.8 s). Roughly linear. Where the time goes, after optimisation:
inference ~45%, repair ~15%, verification ~8%, profiling and serialisation the
rest.

The inference was halved by removing duplicated work, not by cleverness: the
same `astype(str).str.strip().str.lower()` was recomputed four times per column,
`drop_duplicates` ran twice, and `_numerique` chained four separate
`str.replace` passes where a fast path plus one regex does. `_Colonne` caches
the normalised text and the numeric reading per column; keep using it rather
than reaching for the raw series.

**Do not re-attempt the single-regex marker match.** Replacing
`strip().lower().isin()` with one `fullmatch` over an alternation of all markers
looks obviously better and measured 10.6 s against 9.1 — but run-to-run sd is
0.8 s, so it demonstrated nothing. It is documented in `contract.est_vide` so
nobody spends the afternoon again.

`/contract-clean/` caps uploads at **25 MB**, not the 100 MB the other routes
announce: at 1.8 MB/s a 100 MB file needs nearly a minute, which no ordinary
HTTP request survives — the client would get nothing at all. Going higher needs
background processing with a job id, which is a different product.

`reparer` applies five passes **in a fixed order** — empty markers, whitespace
then case, declared types, dedup, imputation. The order is the safety: clearing
markers first is what makes a mode impossible to poison. There is deliberately
**no loop**: the repairs are deterministic and dependency-ordered, so a second
pass changes nothing, and what remains is either refused on purpose or a failure
to report.

### Backend (`backend/app/`)

**`routers/analyze.py`** — Accepts CSV/Excel (max 100MB), delegates to `DataProfiler`, returns issues + column info.

**`services/profiler.py`** — Core analysis engine. Auto-detects encoding (UTF-8/Latin-1/cp1252) and delimiter (comma/semicolon). Normalizes column names to snake_case. Classifies each column's semantic type: `numeric`, `numeric-mixed`, `categorical`, `id`, `datetime`, `datetime-mixed`, `text`. Detects 25+ null-string patterns beyond NaN. Issue severity: `critical` (>30% missing) / `high` / `medium` / `low`.

**`routers/suggest.py`** — Rule-based strategies per issue type. Each strategy includes `name`, `pros`, `cons`, and `code_preview` (the actual code snippet used for generation). The `recommended` index points to the best strategy.

**`routers/generate.py`** — Assembles a complete, standalone `.py` script from the selected strategies' `code_preview` fields. Validates syntax with Python's `ast` module before returning. Auto-corrects deprecated pandas methods (e.g. `fillna(method='ffill')` → `ffill()`). Generated scripts include their own `detect_encoding`/`detect_delimiter`/`load_data` helpers so they run without this app.

**`routers/execute.py`** — Runs a cleaning script on the uploaded file, in a temp dir, 30s timeout. The script comes from the **client** (the user may edit what was generated), so two barriers apply, in `services/sandbox.py`:
- `subprocess_env()` builds the child environment instead of inheriting it. Before this, the script ran with the server's `OPENAI_API_KEY` and `stdout` was returned to the caller — three lines exfiltrated the key. Keep `APPDATA`/`HOME` in the kept list: Python needs them to find the *user site*, and without them `import pandas` fails.
- `check_script()` refuses anything outside the shape the generator produces (import allowlist, no `eval`/`exec`/`__import__`/`getattr`, no dunder climbing). This is defence in depth, **not a sandbox** — arbitrary Python is arbitrary Python. What protects is the empty environment.

**`config.py`** — `OPENAI_API_KEY` feeds `services/advisor.py` (the LLM picks and justifies a strategy, fail-open to rule-based).

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
