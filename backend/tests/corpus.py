"""La salle de torture : un fichier par pathologie, et ce qu'on en attend.

## Pourquoi un corpus fabriqué plutôt que des fichiers trouvés

Sept fichiers réels ont trouvé deux vrais bogues — un plantage sur les colonnes
booléennes, et une colonne de codes convertie en entiers qui vidait 424 lignes.
Ils en trouveront d'autres, mais ils ont deux défauts : ils arrivent par paquets
de pathologies mêlées, donc on ne sait pas laquelle a cassé quoi ; et ils ne
couvrent que ce que le hasard a mis dedans.

Ici, chaque fichier porte **une** pathologie, nommée, avec ce qu'on attend en
sortie. Quand un cas casse, on sait immédiatement lequel. Et quand on en corrige
un, le corpus reste comme test de non-régression.

Les deux approches sont complémentaires : le corpus couvre ce qu'on sait, les
fichiers réels couvrent ce qu'on ignore.

## Ce qu'un cas déclare

`attendu` n'est pas « le contrat tient » — sur plusieurs de ces fichiers, le bon
comportement est justement de **refuser** quelque chose. C'est une fonction qui
reçoit le résultat de la passe et dit si elle est contente, ce qui permet
d'exprimer « cette colonne doit ressortir en texte » aussi bien que « aucune
ligne ne doit disparaître ».
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List


@dataclass
class Cas:
    """Un fichier, sa pathologie, et le verdict attendu."""
    nom: str
    pathologie: str
    octets: bytes
    #: `(resultat) -> None`, qui lève `AssertionError` si la sortie ne convient pas.
    attendu: Callable[[Dict[str, Any]], None] = lambda r: None
    #: Pourquoi ce cas existe — ce qu'il casserait s'il n'était pas traité.
    enjeu: str = ""


def _csv(lignes: List[str], encodage: str = "utf-8", bom: bool = False) -> bytes:
    texte = "\n".join(lignes) + "\n"
    brut = texte.encode(encodage)
    return (b"\xef\xbb\xbf" + brut) if bom else brut


def _colonnes(r: Dict[str, Any]) -> Dict[str, str]:
    return {c["nom"]: c["type"] for c in r["contrat"]["colonnes"]}


def _lire_csv(r: Dict[str, Any]):
    import base64
    import io

    import pandas as pd
    return pd.read_csv(io.BytesIO(base64.b64decode(r["csv_base64"])), dtype=str)


# --------------------------------------------------------------------------- #
# Les cas
# --------------------------------------------------------------------------- #
def _zeros_en_tete(r):
    d = _lire_csv(r)
    assert _colonnes(r)["code_postal"] == "string", _colonnes(r)
    assert "01000" in set(d["code_postal"]), sorted(set(d["code_postal"]))[:5]


def _identifiant_long(r):
    d = _lire_csv(r)
    attendu = {f"040130{i:05d}" for i in range(40)}
    assert set(d["geoid"]) == attendu, sorted(set(d["geoid"]))[:3]


def _devise(r):
    assert _colonnes(r)["montant"] in ("number", "integer", "string")


def _comptable(r):
    """Le signe ne doit pas disparaître, et le piège doit être nommé."""
    assert _colonnes(r)["solde"] not in ("number", "integer"), _colonnes(r)
    assert any("parenthèses" in n for n in r["arbitrages"]), r["arbitrages"]


def _aucune_ligne_perdue(n: int):
    def verifier(r):
        assert r["lignes"][1] == n, f"{r['lignes'][1]} lignes au lieu de {n}"
    return verifier


def _colonne_vide_signalee(r):
    assert any("vides à plus de 60" in n or "entièrement vide" in n
               for n in r["arbitrages"] + r["enonce"]["refuse_exprès"]), r["arbitrages"]


def corpus() -> List[Cas]:
    """Les cas, dans l'ordre où ils font mal."""
    cas: List[Cas] = []

    cas.append(Cas(
        "zeros-en-tete.csv",
        "codes postaux à zéro initial",
        _csv(["id,code_postal,ville"] +
             [f"{i},0{1000 + i:04d},V{i}" for i in range(40)]),
        _zeros_en_tete,
        "Le piège le plus coûteux du métier : « 01000 » se convertit en entier "
        "sans erreur et devient 1000. Rien ne signale la perte, et toute "
        "jointure sur ce code échoue ensuite en silence."))

    cas.append(Cas(
        "geoid.csv",
        "identifiants de recensement à 11 chiffres",
        _csv(["geoid,pop"] + [f"040130{i:05d},{100 + i}" for i in range(40)]),
        _identifiant_long,
        "Le défi Bias Bounty prévient explicitement de ce cas : lire un GEOID "
        "comme un entier mange le zéro initial des États dont le FIPS commence "
        "par 0 — l'Arizona est 04 — et casse toutes les jointures. La première "
        "version de ce cas utilisait le FIPS 40 (Oklahoma), qui n'a pas de zéro "
        "initial : le cas passait sans rien éprouver."))

    cas.append(Cas(
        "bom.csv",
        "UTF-8 avec marque d'ordre des octets",
        _csv(["id,nom", "1,Alice", "2,Bob"] + [f"{i},N{i}" for i in range(3, 25)],
             bom=True),
        lambda r: (_ for _ in ()).throw(AssertionError(
            f"première colonne : {list(_colonnes(r))[0]!r}"))
        if list(_colonnes(r))[0] not in ("id",) else None,
        "Le BOM colle un caractère invisible au premier nom de colonne. On ne "
        "le voit ni à l'écran ni dans un diff, et tout accès par nom échoue."))

    cas.append(Cas(
        "europeen.csv",
        "point-virgule, virgule décimale, espace de milliers",
        _csv(["id;montant;taux"] +
             [f"{i};1 2{i:02d},50;3,5" for i in range(30)], encodage="utf-8"),
        _devise,
        "Un export Excel français. Le délimiteur, le séparateur décimal et le "
        "séparateur de milliers changent tous les trois en même temps."))

    cas.append(Cas(
        "latin1.csv",
        "encodage Latin-1 avec accents",
        _csv(["id,ville"] + [f"{i},Ch\xe2teauroux" for i in range(30)],
             encodage="latin-1"),
        lambda r: None,
        "Lu en UTF-8, le fichier lève ou produit du charabia. Le chargeur "
        "essaie plusieurs encodages ; encore faut-il que ça marche jusqu'au "
        "bout de la passe."))

    cas.append(Cas(
        "devises.csv",
        "symboles monétaires et pourcentages",
        _csv(["id,montant,part"] +
             [f"{i},€1 2{i:02d},{i}%" for i in range(30)]),
        _devise,
        "« €1 200 » et « 45 % » sont des nombres pour un humain et du texte "
        "pour pandas. Les convertir sans le dire perd l'unité ; ne pas les "
        "convertir rend la colonne inutilisable."))

    cas.append(Cas(
        "comptable.csv",
        "négatifs entre parenthèses",
        _csv(["id,solde"] +
             [f"{i},{'(1200)' if i % 3 else '1200'}" for i in range(30)]),
        _comptable,
        "La convention comptable écrit -1200 « (1200) ». Lu naïvement, le "
        "signe disparaît : un débit devient un crédit."))

    cas.append(Cas(
        "booleens-mixtes.csv",
        "booléens en trois conventions",
        _csv(["id,actif,valide,ok"] +
             [f"{i},{'Oui' if i % 2 else 'Non'},{'Y' if i % 2 else 'N'},"
              f"{1 if i % 2 else 0}" for i in range(30)]),
        lambda r: None,
        "« Oui/Non », « Y/N » et « 1/0 » disent la même chose. Les traiter "
        "comme trois types différents disperse la même information."))

    cas.append(Cas(
        "dates-melangees.csv",
        "quatre formats de date dans une colonne",
        _csv(["id,quand"] + [
            f"{i}," + ["2026-01-0{}".format(i % 9 + 1),
                       "0{}/01/2026".format(i % 9 + 1),
                       "1 janvier 2026",
                       "2026/01/0{}".format(i % 9 + 1)][i % 4]
            for i in range(32)]),
        lambda r: None,
        "Un fichier recollé depuis plusieurs sources. Aucun format ne tient "
        "sur toute la colonne, donc le contrat doit reculer sur du texte "
        "plutôt que de vider les trois quarts des lignes."))

    cas.append(Cas(
        "colonnes-dupliquees.csv",
        "deux colonnes du même nom",
        _csv(["id,valeur,valeur"] + [f"{i},{i},{i * 2}" for i in range(30)]),
        lambda r: None,
        "pandas renomme la seconde en `valeur.1` et personne ne le remarque. "
        "Le contrat porte alors sur une colonne que l'utilisateur ne croit pas "
        "avoir."))

    cas.append(Cas(
        "colonne-sans-nom.csv",
        "un en-tête vide",
        _csv(["id,,montant"] + [f"{i},x,{i}" for i in range(30)]),
        lambda r: None,
        "Un export qui laisse une colonne sans titre. Le nom normalisé peut "
        "devenir vide, et une clé de dictionnaire vide casse les rapports."))

    cas.append(Cas(
        "colonne-vide.csv",
        "une colonne entièrement vide",
        _csv(["id,plein,vide"] + [f"{i},{i}," for i in range(30)]),
        _colonne_vide_signalee,
        "Rien à nettoyer, rien à imputer. Le bon geste est de le dire, pas de "
        "fabriquer une valeur ni de planter."))

    cas.append(Cas(
        "colonne-constante.csv",
        "une colonne à valeur unique",
        _csv(["id,constante"] + [f"{i},7" for i in range(30)]),
        lambda r: None,
        "L'écart interquartile est nul : une règle d'aberrance naïve déclare "
        "aberrante toute valeur différente de la médiane, c'est-à-dire aucune "
        "ou toutes."))

    cas.append(Cas(
        "une-seule-ligne.csv",
        "une ligne de données",
        _csv(["id,valeur", "1,42"]),
        lambda r: None,
        "Les quantiles, la médiane et la détection de clé n'ont pas de sens "
        "sur une ligne. Rien ne doit lever."))

    cas.append(Cas(
        "entete-seul.csv",
        "aucune ligne de données",
        _csv(["id,valeur"]),
        lambda r: None,
        "Un fichier vide est un état légitime avant le premier chargement et "
        "catastrophique après. L'outil ne peut pas trancher, il doit le dire."))

    cas.append(Cas(
        "guillemets.csv",
        "retours à la ligne dans un champ cité",
        _csv(['id,commentaire'] +
             [f'{i},"ligne un\nligne deux"' for i in range(25)]),
        _aucune_ligne_perdue(25),
        "Un champ cité peut contenir un retour à la ligne. Compter les lignes "
        "du fichier au lieu de les parser donne le double."))

    cas.append(Cas(
        "espaces-partout.csv",
        "espaces avant et après chaque valeur",
        _csv(["id , ville , montant "] +
             [f" {i} ,  Rabat  , {i} " for i in range(30)]),
        lambda r: None,
        "Les espaces se glissent dans les noms de colonnes ET dans les "
        "valeurs. Une clé « Rabat » et une clé «  Rabat  » sont deux clés."))

    cas.append(Cas(
        "nuls-multilingues.csv",
        "marqueurs de vide en trois langues",
        _csv(["id,a,b,c"] + [
            f"{i},{'inconnu' if i % 3 == 0 else i},"
            f"{'N/A' if i % 3 == 1 else i},{'néant' if i % 3 == 2 else i}"
            for i in range(30)]),
        lambda r: None,
        "« inconnu », « N/A » et « néant » veulent dire vide. Un seul oublié "
        "devient le mode de sa colonne et se fait imputer partout."))

    cas.append(Cas(
        "large.csv",
        "trois cents colonnes",
        _csv([",".join(f"c{j}" for j in range(300))] +
             [",".join(str(i * j % 97) for j in range(300)) for i in range(40)]),
        lambda r: None,
        "Un fichier large coûte par colonne, pas par ligne. C'est là que les "
        "traitements par colonne deviennent visibles."))

    cas.append(Cas(
        "scientifique.csv",
        "notation scientifique",
        _csv(["id,mesure"] + [f"{i},{i}.5e-0{i % 8 + 1}" for i in range(30)]),
        lambda r: None,
        "« 1.5e-07 » est un nombre. Traité en texte, il casse toute "
        "statistique ; arrondi, il perd ce qui le rend intéressant."))

    return cas
