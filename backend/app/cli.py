"""La barrière, en ligne de commande.

    python -m app.cli check clients.csv --usage base
    python -m app.cli check livraison.csv --contrat clients_contrat.json

Faite pour être lue par une chaîne de chargement ou une CI, donc tout tient
dans le **code de sortie** :

    0   le contrat est tenu : le fichier peut entrer
    1   le contrat n'est pas tenu : il ne doit pas entrer tel quel
    2   on n'a pas pu en juger (fichier absent ou illisible, usage inconnu,
        contrat invalide)

1 et 2 sont distincts exprès : une panne qui sortirait 1 passerait pour un
verdict, et un verdict qui sortirait 2 pour une panne.

Rien n'est écrit sur le disque sans `--sortie` : une barrière qui laisse des
fichiers derrière elle à chaque contrôle finit par polluer ce qu'elle garde.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.services import agent_contrat, contract, contract_infer

TENU, NON_TENU, INDECIDABLE = 0, 1, 2
EXTENSIONS = {".csv", ".xlsx", ".xls"}


class Indecidable(Exception):
    """On ne peut pas rendre de verdict — ce n'est pas un « non tenu »."""


def _charger_contrat(chemin: Optional[str]) -> Optional[Dict[str, Any]]:
    if not chemin:
        return None
    p = Path(chemin)
    if not p.is_file():
        raise Indecidable(f"contrat introuvable : {p}")
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        contract.Contrat.from_dict(d)
    except Exception as e:
        raise Indecidable(f"contrat invalide ({p.name}) : {e}")
    return d


def controler(fichier: str, usage: str, contrat: Optional[str]) -> Dict[str, Any]:
    p = Path(fichier)
    if not p.is_file():
        raise Indecidable(f"fichier introuvable : {p}")
    if p.suffix.lower() not in EXTENSIONS:
        raise Indecidable(f"format non pris en charge : {p.suffix or '(aucun)'} "
                          f"(attendu : {', '.join(sorted(EXTENSIONS))})")
    declare = _charger_contrat(contrat)
    if declare is None and usage not in contract_infer.USAGES:
        raise Indecidable(f"usage inconnu : {usage!r} "
                          f"(attendu : {', '.join(sorted(contract_infer.USAGES))})")
    try:
        return asyncio.run(agent_contrat.run(p.read_bytes(), p.name,
                                             usage=usage, contrat=declare))
    except ValueError as e:
        raise Indecidable(f"{p.name} : {e}")
    except Exception as e:
        raise Indecidable(f"{p.name} : lecture ou traitement impossible ({e})")


def _verdict_json(res: Dict[str, Any], fichier: str) -> Dict[str, Any]:
    """Le verdict seul : ni les données ni le script, qui pèsent et ne sont
    pas ce qu'une chaîne vient chercher."""
    return {
        "fichier": fichier,
        "usage": res["usage"],
        "contrat_tenu": res["contrat_tenu"],
        "score": res["score_apres"],
        "lignes": res["lignes"],
        "violations": res["rapport"]["violations"],
        "garanti": res["enonce"]["garanti"],
        "refuse": res["enonce"]["refuse_exprès"],
        "repare": res["enonce"]["fait"],
    }


def _liste(titre: str, items: List[str], marque: str, limite: int = 12) -> List[str]:
    if not items:
        return []
    lignes = [f"{titre} ({len(items)})"]
    lignes += [f"  {marque} {t}" for t in items[:limite]]
    if len(items) > limite:
        lignes.append(f"    ... et {len(items) - limite} autre(s) (--json pour tout voir)")
    return lignes + [""]


def _texte(res: Dict[str, Any], fichier: str) -> str:
    libelle = contract_infer.USAGES.get(res["usage"], {}).get("libelle", res["usage"] or "contrat fourni")
    tete = "TENU" if res["contrat_tenu"] else "NON TENU"
    lignes = [
        f"{fichier} — usage : {libelle}",
        f"Contrat {tete} ({res['score_apres']}/100) — "
        f"{res['lignes'][1]} lignes sur {res['lignes'][0]} au départ",
        "",
    ]
    lignes += _liste("Non garanti", res["enonce"]["non_garanti"], "x")
    lignes += _liste("Refusé exprès", res["enonce"]["refuse_exprès"], "!")
    lignes += _liste("Réparé", res["enonce"]["fait"], "-", limite=6)
    lignes.append(f"Garanti : {len(res['enonce']['garanti'])} contrôle(s) vérifié(s) sur les données")
    return "\n".join(lignes)


def _ecrire(res: Dict[str, Any], fichier: str, dossier: str) -> List[Path]:
    d = Path(dossier)
    d.mkdir(parents=True, exist_ok=True)
    base = Path(fichier).stem
    ecrits = []

    def poser(nom: str, octets: bytes) -> None:
        (d / nom).write_bytes(octets)
        ecrits.append(d / nom)

    poser(f"{base}_conforme.csv", base64.b64decode(res["csv_base64"]))
    if res.get("parquet_base64"):
        poser(f"{base}_conforme.parquet", base64.b64decode(res["parquet_base64"]))
    poser(f"{base}_contrat.json",
          json.dumps(res["contrat"], ensure_ascii=False, indent=2).encode("utf-8"))
    return ecrits


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="Contrôle un fichier tabulaire contre un contrat de données. "
                    "Code de sortie : 0 tenu, 1 non tenu, 2 indécidable.")
    sous = ap.add_subparsers(dest="commande", required=True)
    c = sous.add_parser("check", help="ce fichier peut-il entrer ?")
    c.add_argument("fichier", help="CSV ou Excel à contrôler")
    c.add_argument("--usage", default="base",
                   help="base (défaut), jointure, modele ou tableau")
    c.add_argument("--contrat", help="contrat JSON à appliquer au lieu de l'inférer "
                                     "(celui écrit par --sortie)")
    c.add_argument("--json", action="store_true", help="verdict en JSON sur la sortie standard")
    c.add_argument("--sortie", metavar="DOSSIER",
                   help="écrire là les données réparées, le Parquet et le contrat")
    # Une console qui ne sait pas écrire un caractère ne doit pas transformer
    # un verdict en trace d'erreur ; et les avertissements de pandas n'ont rien
    # à faire dans la sortie d'une barrière.
    for flux in (sys.stdout, sys.stderr):
        if hasattr(flux, "reconfigure"):
            flux.reconfigure(errors="replace")
    warnings.simplefilter("ignore")
    try:
        args = ap.parse_args(argv)
    except SystemExit as e:           # argparse sort 2 sur une erreur : c'est le bon code
        return INDECIDABLE if e.code else 0

    try:
        res = controler(args.fichier, args.usage, args.contrat)
        ecrits = _ecrire(res, args.fichier, args.sortie) if args.sortie else []
    except Indecidable as e:
        print(f"indécidable : {e}", file=sys.stderr)
        return INDECIDABLE

    nom = Path(args.fichier).name
    if args.json:
        print(json.dumps(_verdict_json(res, nom), ensure_ascii=False, indent=2))
    else:
        print(_texte(res, nom))
        for p in ecrits:
            print(f"écrit : {p}")
    return TENU if res["contrat_tenu"] else NON_TENU


if __name__ == "__main__":
    sys.exit(main())
