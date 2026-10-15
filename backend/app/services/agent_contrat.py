"""Le prétraitement sous contrat : profiler, déclarer, réparer, vérifier, livrer.

## Pourquoi il n'y a plus de boucle

`agent_cleaner` itérait : nettoyer, reprofiler, recommencer tant que le score
montait. Cette boucle existait parce que les réparations étaient aveugles — elles
visaient un problème détecté sans savoir ce que la donnée devait garantir, donc
il fallait tâtonner et garder la meilleure version.

Les réparations de `reparer` visent des clauses, dans un ordre qui tient compte
de leurs dépendances. Une seconde passe du même traitement déterministe ne
change rien. Ce qui reste après la passe est soit **refusé exprès** (un budget
l'interdit), soit un échec à rapporter. Dans les deux cas, itérer ne le
résoudrait pas : ça ne ferait que masquer la raison.

## Ce que livre `run`

Trois choses, et la troisième est celle qui manquait :

1. les données réparées (CSV, et Parquet où les types survivent) ;
2. le script reproductible ;
3. **l'énoncé de ce qui est garanti, et de ce qui ne l'est pas.** Un data
   engineer ne remet pas un score, il remet des garanties. « Cette colonne tient
   le type entier, cette clé identifie chaque ligne, et je n'ai pas pu garantir
   le remplissage de celle-là, voici pourquoi. »
"""
from __future__ import annotations

import base64
import io
from typing import Any, Dict, List, Optional

import pandas as pd

from app.services import contract, contract_infer, reparer
from app.services.profiler import DataProfiler


async def _profiler(content: bytes, filename: str):
    """Profile le fichier **lu en texte**.

    Laisser pandas typer à la lecture détruit les codes à zéro initial avant
    tout contrôle : `01000` devient `1000`, et la colonne tient parfaitement
    son type, donc rien ne peut plus le voir. La passe sous contrat lit donc
    tout en chaînes et décide elle-même, ce qui est de toute façon sa raison
    d'être — c'est la troisième fois dans ce module qu'on refuse d'hériter de
    ce que le chargeur a déjà tranché.
    """
    p = DataProfiler()
    analyse = await p.analyze_file(content, filename, texte_brut=True)
    return analyse, p.df


def _parquet(df: pd.DataFrame) -> Optional[str]:
    """Parquet en base64, ou `None` si le moteur manque.

    Le CSV reperd les types à la première relecture : c'est exactement le défaut
    qu'on vient de corriger, et le livrer en CSV seul l'annulerait. Parquet les
    conserve. On ne fait pas échouer la passe si `pyarrow` n'est pas installé —
    on le signale.
    """
    try:
        tampon = io.BytesIO()
        df.to_parquet(tampon, index=False)
        return base64.b64encode(tampon.getvalue()).decode()
    except Exception:
        return None


def _script(contrat: contract.Contrat, journal: reparer.Journal,
            filename: str) -> str:
    """Le script qui rejoue la passe, contrat inclus en commentaire.

    Il importe `reparer` plutôt que de recopier les correctifs : un script qui
    duplique la logique dérive dès la première correction de celle-ci, et c'est
    précisément ce qu'on reproche à l'ancien générateur.
    """
    lignes = [
        '"""Prétraitement sous contrat — généré automatiquement.',
        "",
        f"Source : {filename}",
        f"Usage déclaré : {contrat.usage or 'non précisé'}",
        "",
        "Ce que la passe a refusé de faire :",
    ]
    lignes += [f"  - {g.colonne or 'global'} : {g.detail}"
               for g in journal.refus] or ["  (rien)"]
    lignes += [
        '"""',
        "import json",
        "import pandas as pd",
        "",
        "from app.services.contract import Contrat, verify",
        "from app.services.reparer import appliquer",
        "",
        "CONTRAT = json.loads(r'''",
        __import__("json").dumps(contrat.to_dict(), ensure_ascii=False, indent=2),
        "''')",
        "",
        "",
        "def main(chemin):",
        "    brut = pd.read_csv(chemin, dtype=str)",
        "    contrat = Contrat.from_dict(CONTRAT)",
        "    propre, journal = appliquer(brut, contrat)",
        "    rapport = verify(propre, contrat, origine=brut)",
        "    print(f'contrat tenu : {rapport.tient} ({rapport.score}/100)')",
        "    for v in rapport.violations:",
        "        print(' -', v.message)",
        "    propre.to_parquet('propre.parquet', index=False)",
        "    return propre",
        "",
        "",
        "if __name__ == '__main__':",
        "    import sys",
        "    main(sys.argv[1])",
    ]
    return "\n".join(lignes)


def enonce(rapport: contract.Rapport, journal: reparer.Journal) -> Dict[str, List[str]]:
    """Le livrable en clair : ce qui tient, ce qui ne tient pas, ce qui a été
    refusé exprès.

    La distinction entre « ne tient pas » et « refusé exprès » est le cœur du
    rendu. Un budget qui empêche de combler une colonne n'est pas un échec du
    traitement : c'est une propriété de la donnée, et la dire est utile. Un type
    qui ne tient pas, lui, est un vrai problème à corriger.
    """
    return {
        "garanti": list(rapport.satisfait),
        "non_garanti": [v.message for v in rapport.violations],
        "refuse_exprès": [f"{g.colonne or 'global'} : {g.detail}"
                          for g in journal.refus],
        "fait": [f"{g.colonne or 'global'} : {g.detail}" for g in journal.faits],
    }


async def run(content: bytes, filename: str, usage: str = "modele",
              contrat: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Une passe complète. `contrat` permet de passer un contrat édité par
    l'utilisateur ; sans lui, il est inféré de l'usage déclaré."""
    analyse, origine = await _profiler(content, filename)

    if contrat is not None:
        ct, notes = contract.Contrat.from_dict(contrat), []
    else:
        ct, notes = contract_infer.inferer(analyse, origine, usage=usage)

    avant = contract.verify(origine, ct, origine=None)
    propre, journal = reparer.appliquer(origine, ct)
    apres = contract.verify(propre, ct, origine=origine)

    return {
        "usage": ct.usage,
        "contrat": ct.to_dict(),
        "contrat_lisible": contract_infer.resume(ct),
        "arbitrages": notes,
        "score_avant": avant.score,
        "score_apres": apres.score,
        "contrat_tenu": apres.tient,
        "violations_avant": len(avant.violations),
        "violations_apres": len(apres.violations),
        "rapport": apres.to_dict(),
        "journal": journal.to_dict(),
        "enonce": enonce(apres, journal),
        "lignes": [len(origine), len(propre)],
        "colonnes": [len(origine.columns), len(propre.columns)],
        "csv_base64": base64.b64encode(
            propre.to_csv(index=False).encode()).decode(),
        "parquet_base64": _parquet(propre),
        "script": _script(ct, journal, filename),
        "message": _message(avant, apres, journal),
    }


def _message(avant: contract.Rapport, apres: contract.Rapport,
             journal: reparer.Journal) -> str:
    if apres.tient:
        return (f"Contrat tenu ({apres.score}/100). "
                f"{len(apres.satisfait)} garantie(s) vérifiée(s), "
                f"{len(journal.faits)} réparation(s).")
    bloquantes = len(apres.bloquantes)
    detail = f"{bloquantes} bloquante(s)" if bloquantes else "aucune bloquante"
    return (f"Contrat non tenu : {len(apres.violations)} clause(s) en défaut "
            f"({detail}), contre {len(avant.violations)} avant. "
            f"{len(journal.refus)} réparation(s) refusée(s) par budget — "
            f"voir « refusé exprès ».")
