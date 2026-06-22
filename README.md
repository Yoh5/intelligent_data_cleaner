# Intelligent Data Cleaner

Plateforme web d'analyse et de nettoyage automatisé de données CSV/Excel. Détecte les problèmes de qualité de données, propose des stratégies de nettoyage contextuelles et génère des scripts Python autonomes, validés syntaxiquement et prêts à l'emploi.

![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)
![Next.js](https://img.shields.io/badge/Next.js-16+-black.svg)
![React](https://img.shields.io/badge/React-19+-61DAFB.svg)
![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-3178C6.svg)

---

## Fonctionnalités

### Analyse de données
- Chargement CSV/Excel avec auto-détection de l'encodage (UTF-8, Latin-1, cp1252) et du délimiteur (`,` `;`)
- Détection de 25+ patterns de valeurs manquantes (`NA`, `null`, `N/A`, `???`, espaces, etc.)
- Inférence du type sémantique par colonne : `numeric`, `numeric-mixed`, `categorical`, `id`, `datetime`, `datetime-mixed`, `text`
- Détection de 6 catégories de problèmes :
  - Valeurs manquantes (sévérité adaptive : low → critical selon le taux)
  - Types mixtes (nombres stockés en texte)
  - Lignes entièrement dupliquées
  - Doublons sur colonnes identifiant
  - Valeurs aberrantes (règle 3×IQR sur colonnes numériques)
  - Espaces superflus (texte/catégoriel)
  - Dates stockées en texte

### Stratégies de nettoyage
Chaque problème reçoit 2 à 4 stratégies adaptées au type sémantique de la colonne :

| Problème | Stratégies |
|---|---|
| Valeurs manquantes numériques | Médiane / Moyenne / ffill / Supprimer lignes |
| Valeurs manquantes catégorielles | Mode / Constante "Inconnu" / Supprimer lignes |
| Valeurs manquantes datetime | ffill / Supprimer lignes |
| Outliers | Écrêtage IQR / Écrêtage percentile / Supprimer lignes |
| Espaces | Strip / Strip + minuscules |
| Dates en texte | `pd.to_datetime()` |
| Types mixtes | Conversion numérique avec gestion des virgules/devises |
| Doublons | Garder premier / Garder dernier |

### Génération de scripts
- Scripts Python autonomes (incluent leurs propres helpers `load_data`, `detect_encoding`, `detect_delimiter`)
- Validation AST avant export (garantie syntaxique)
- Auto-correction des méthodes Pandas dépréciées (ex: `fillna(method='ffill')` → `ffill()`)
- Fallback garanti : chaque colonne a toujours un code de secours valide

### Exécution & téléchargement
- Exécution du script directement depuis l'interface sur le fichier original
- Retourne `.xlsx` si l'entrée était Excel, `.csv` sinon
- Statistiques avant/après : lignes, nulls corrigés, taux de correction

### Modes d'utilisation

**Manuel** — Upload → Analyse → Choisir une stratégie par problème → Générer le script → Exécuter & Télécharger

**Autopilot** — Upload → Nettoyage automatique en un clic (détecte + choisit + génère en une seule requête)

**URL** — Analyse depuis un lien direct CSV/Excel, une URL Google Sheets (conversion automatique export CSV), ou un S3 public

---

## Lancement

### Prérequis
- Python 3.12+
- Node.js 18+

### Backend
```bash
cd backend
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # macOS/Linux

pip install -r requirements.txt
uvicorn app.main:app --reload  # http://localhost:8000/docs
```

### Frontend
```bash
cd frontend
npm install
npm run dev    # http://localhost:3000
npm run build
npm run lint
```

### Docker (stack complète)
```bash
docker-compose up --build
```

### Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `OPENAI_API_KEY` | — | Clé OpenAI (réservée — suggestions actuellement rule-based) |
| `CORS_ORIGINS` | `["http://localhost:3000","http://localhost:5173"]` | Origines autorisées |
| `DEBUG` | `false` | Mode debug FastAPI |
| `NEXT_PUBLIC_API_URL` | `http://localhost:8000` | URL du backend (frontend) |

Créez un fichier `backend/.env` pour surcharger les valeurs par défaut.

---

## Architecture

```
POST /analyze/        → DataProfiler → profil complet + issues
POST /analyze/url     → fetch HTTP → DataProfiler (même pipeline)
POST /suggest/batch   → stratégies rule-based par issue + type sémantique
POST /generate/       → assemblage + validation AST → script .py
POST /auto-clean/     → analyze + suggest + generate en une passe
POST /execute/        → subprocess.run du script + retour fichier nettoyé
```

### Backend (`backend/app/`)

- **`services/profiler.py`** — Moteur central. Charge, normalise les noms de colonnes (supprime les caractères spéciaux pouvant casser les string literals Python), infère les types sémantiques, détecte les problèmes. Expose `sample_rows` pour l'aperçu frontend.
- **`routers/suggest.py`** — Stratégies rule-based contextuelles. Chaque stratégie expose `code_preview` (le code exact qui sera injecté dans le script final).
- **`routers/generate.py`** — Assemble le script complet, valide avec `ast.parse`, auto-corrige les méthodes dépréciées.
- **`routers/auto_clean.py`** — Orchestrateur : appelle le profiler, choisit la première stratégie recommandée pour chaque issue, génère le script en une passe.
- **`routers/execute.py`** — Exécute le script dans un thread pool (`run_in_executor`) pour compatibilité Windows/uvicorn (pas d'`asyncio.create_subprocess_exec`). Retourne le fichier nettoyé en base64 + statistiques.
- **`routers/analyze.py`** — Upload et ingestion URL avec protection SSRF (schémas http/https uniquement, IPs privées bloquées).

### Frontend (`frontend/`)

Next.js 16 App Router + React 19 + Tailwind CSS.

- **`app/page.tsx`** — Orchestre trois états : `idle` (upload) → `analysis` (pipeline manuel) ou `autopilot`. Health check backend au montage.
- **`components/FileUpload.tsx`** — Drag-and-drop, sélection fichier, onglet URL avec validation client.
- **`components/AnalysisResult.tsx`** — Appelle `/suggest/batch` au montage (auto-sélection recommandée), aperçu des données, bouton "Tout sélectionner", génération du script.
- **`components/SuggestionModal.tsx`** — Sélecteur de stratégie avec pros/cons, code preview, fermeture Escape/backdrop.
- **`components/CodeExport.tsx`** — Affiche le script, boutons Copier/Télécharger .py/Exécuter, panel de stats post-exécution.
- **`lib/api.ts`** — Deux instances Axios (`api` JSON, `apiFormData` multipart) avec timeouts, intercepteur d'erreur centralisé, `checkBackendHealth()`.

---

## Sécurité

- **SSRF** : `/analyze/url` accepte uniquement `http://` et `https://`, les IPs privées (10.x, 192.168.x, 127.x, link-local) sont bloquées.
- **Injection de code** : Les noms de colonnes sont sanitizés (apostrophes, guillemets, caractères spéciaux supprimés) avant toute génération de code.
- **Taille** : Fichiers limités à 100 MB côté serveur.
- **Timeout** : Exécution de script plafonnée à 30 secondes.
- **CORS** : Origines configurables via variable d'environnement (non hardcodées).

> L'exécution de scripts se fait avec les droits du processus serveur. En production, isoler dans un conteneur ou un sandbox.
