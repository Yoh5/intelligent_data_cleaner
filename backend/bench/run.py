"""Lancer le banc. Depuis `backend/` :

    python -m bench.run                       # tous les jeux, tous les nettoyeurs
    python -m bench.run --jeu synthetique --graines 1 2 3
    python -m bench.run --json resultats.json

Le jeu Titanic est sauté s'il n'est pas dans `corpus-public/`.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import List

from bench import concurrents, donnees
from bench.defauts import abimer
from bench.mesure import Mesure, mesurer

CORPUS = Path(__file__).resolve().parents[2] / "corpus-public"


def jeux(nom: str, graine: int):
    if nom in ("synthetique", "tous"):
        yield donnees.synthetique(graine=graine)
    if nom in ("titanic", "tous"):
        t = donnees.titanic(CORPUS / "titanic.csv", graine=graine)
        if t is not None:
            yield t


def lancer(jeu: str = "tous", graines=(7,), nettoyeurs=None) -> List[Mesure]:
    nettoyeurs = nettoyeurs or list(concurrents.NETTOYEURS)
    mesures: List[Mesure] = []
    for g in graines:
        for verite in jeux(jeu, g):
            ech = abimer(verite, graine=g + 100)
            for n in nettoyeurs:
                (sortie, verdict, detail), duree, err = concurrents.executer(n, ech.octets, f"{verite.nom}.csv")
                profil = concurrents.PROFILS.get(n, "base")
                m = mesurer(ech, sortie, n, profil) if not err else Mesure(n, verite.nom, profil=profil)
                m.verdict_outil, m.verdict_detail, m.duree_s, m.erreur = verdict, detail, round(duree, 2), err
                mesures.append(m)
    return mesures


def _pct(a: int, b: int) -> str:
    return "—" if not b else f"{100 * a / b:.1f} %"


def tableau(mesures: List[Mesure]) -> str:
    l = ["| jeu | nettoyeur (jugé pour) | défauts corrigés | cellules saines cassées "
         "| inventées silencieuses / déclarées | doublons restants | lignes perdues | prête ? "
         "| verdict de l'outil | juste ? |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for m in mesures:
        if m.erreur:
            l.append(f"| {m.jeu} | {m.nettoyeur} | plantage : {m.erreur[:60]} |||||||||")
            continue
        juste = {None: "—", True: "oui", False: "**NON**"}[m.verdict_juste]
        l.append(
            f"| {m.jeu} | {m.nettoyeur} ({m.profil}) | {m.corrigees}/{m.defauts} ({_pct(m.corrigees, m.defauts)}) "
            f"| {m.cassees} ({_pct(m.cassees, m.saines)}) | {m.inventees_silencieuses} / {m.inventees_declarees} "
            f"| {m.doublons_restants}/{m.doublons_injectes} "
            f"| {m.lignes_perdues} | {'oui' if m.prete else 'non'} | {m.verdict_detail} | {juste} |")
    return "\n".join(l)


def detail(mesures: List[Mesure]) -> str:
    l = []
    for m in mesures:
        if m.erreur:
            continue
        l.append(f"\n### {m.jeu} · {m.nettoyeur} ({m.duree_s} s)")
        l.append("par défaut : " + ", ".join(f"{d} {c}/{t}" for d, (c, t) in sorted(m.par_defaut.items())))
        if m.colonnes_perdues:
            l.append(f"colonnes perdues : {', '.join(m.colonnes_perdues)}")
        for e in m.exemples_casses:
            l.append(f"  cassée  · {e}")
        for e in m.exemples_manques:
            l.append(f"  manquée · {e}")
    return "\n".join(l)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.run")
    p.add_argument("--jeu", default="tous", choices=["tous", "synthetique", "titanic"])
    p.add_argument("--graines", nargs="+", type=int, default=[7])
    p.add_argument("--nettoyeurs", nargs="+", choices=list(concurrents.NETTOYEURS))
    p.add_argument("--json", metavar="FICHIER")
    a = p.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    mesures = lancer(a.jeu, a.graines, a.nettoyeurs)
    print(tableau(mesures))
    print(detail(mesures))
    if a.json:
        Path(a.json).write_text(json.dumps(
            [dict(asdict(m), manquees=m.manquees, inventees_silencieuses=m.inventees_silencieuses, prete=m.prete, verdict_juste=m.verdict_juste)
             for m in mesures], ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
