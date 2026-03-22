# Intelligent Data Cleaner

Plateforme web intelligente d'analyse et de nettoyage automatisé de données CSV/Excel. Détecte les problèmes de qualité de données, suggère des stratégies de nettoyage via IA (OpenAI) ou règles intelligentes, et génère des scripts Python prêts à l'emploi, validés syntaxiquement.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)
![React](https://img.shields.io/badge/React-18+-61DAFB.svg)
![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-3178C6.svg)
![OpenAI](https://img.shields.io/badge/OpenAI-API-412991.svg)

## Fonctionnalités Clés

### Analyse Intelligente
- **Détection ultra-robuste des valeurs manquantes** : Détecte non seulement les `NaN` standard, mais aussi les chaînes vides, espaces multiples (`'   '`), `'NA'`, `'null'`, `'N/A'` (25+ patterns)
- **Inférence de types intelligente** : Détecte automatiquement les colonnes "numériques avec texte" (ex: `TotalCharges` avec espaces) et les dates mal formatées
- **Détection automatique d'encodage** : Gère UTF-8, Latin-1, Windows-1252, etc.
- **Détection de délimiteurs** : Supporte CSV, TSV, point-virgule automatiquement

### Génération de Code Validée
- **Validation syntaxique AST** : Tout code généré est parsé et validé avant export (détection d'erreurs de syntaxe avant runtime)
- **Auto-correction** : Correction automatique des méthodes dépréciées (ex: `fillna(method='ffill')` → `ffill()`)
- **Conversion de types robuste** : Gestion des séparateurs décimaux français (virgule) et cast implicite avant opérations statistiques
- **Scripts autonomes** : Génération de fichiers `.py` complets avec argument parsing, gestion d'erreurs et logging

### Interface Moderne
- Upload drag & drop de fichiers CSV/Excel
- Visualisation des problèmes détectés avec badges colorés
- Aperçu des données avant/après nettoyage
- Téléchargement du script Python généré


### Stack Technique
- **Backend** : FastAPI, Pandas, NumPy, OpenAI, chardet (détection encodage)
- **Frontend** : React, TypeScript, Tailwind CSS
- **Validation** : AST (Abstract Syntax Tree) pour la vérification de code

## Installation

### Prérequis
- Python 3.9+
- Node.js 18+
- Clé API OpenAI (optionnel mais recommandé)

### Backend
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

--Configuration
cp .env.example .env
# Éditer .env et ajouter votre OPENAI_API_KEY

--Lancement
uvicorn main:app --reload

--Frontend
cd frontend
npm install
npm run dev