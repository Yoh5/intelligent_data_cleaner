"""« Ces deux fichiers sont-ils joignables ? » — le diagnostic, avant la jointure.

Une jointure ne plante jamais. Elle rend **moins** de lignes quand des clés de
gauche ne trouvent rien à droite, **plus** quand une clé est répétée des deux
côtés, et dans les deux cas elle ne dit rien. On mesure donc ce que la
jointure ferait, sans la faire :

- la **cardinalité** réelle (1:1, n:1, 1:n, n:m) — n:m multiplie les lignes ;
- la **couverture** : part des lignes de gauche qui trouvent une ligne à droite ;
- les **presque-correspondances** : les clés orphelines qui trouveraient leur
  ligne si l'on ignorait un espace, la casse ou un zéro initial. C'est le cas
  qui coûte des heures, parce que les deux colonnes « se ressemblent ».

Rien n'est réparé ici : normaliser une clé pour la faire correspondre est une
décision (« 00042 » et « 42 » sont-ils le même client ?), pas un nettoyage.
On nomme les paires et on laisse décider.

Les deux fichiers doivent avoir été lus **en texte** : lus autrement, « 01000 »
et « 1000 » sont déjà devenus égaux et le diagnostic ne voit plus rien.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Tuple

import pandas as pd

from app.services.contract import est_vide

_SEP = "\x1f"          # séparateur de clé composite : absent de toute donnée tabulaire
_EXEMPLES = 5


class CleInconnue(ValueError):
    """La colonne demandée n'existe pas — une erreur d'usage, pas un verdict."""


@dataclass
class Diagnostic:
    cle_gauche: List[str]
    cle_droite: List[str]
    lignes_gauche: int
    lignes_droite: int
    cardinalite: str
    couverture: float
    couverture_min: float
    orphelines: int
    exemples_orphelines: List[str]
    presque: int
    exemples_presque: List[Tuple[str, str]]
    cles_vides_gauche: int
    cles_vides_droite: int
    cles_droite_inutilisees: int
    lignes_resultat: int
    problemes: List[Dict[str, str]] = field(default_factory=list)

    @property
    def joignable(self) -> bool:
        return not self.problemes

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["joignable"] = self.joignable
        return d


def _cle(df: pd.DataFrame, colonnes: List[str], cote: str) -> pd.Series:
    """La clé de chaque ligne, en texte exact ; NA si une composante est vide."""
    absentes = [c for c in colonnes if c not in df.columns]
    if absentes:
        raise CleInconnue(
            f"colonne(s) absente(s) du fichier de {cote} : {', '.join(absentes)} "
            f"(colonnes disponibles : {', '.join(map(str, df.columns))})")
    parties, vide = [], pd.Series(False, index=df.index)
    for c in colonnes:
        s = df[c]
        texte = s.where(s.isna(), s.astype(str))
        vide |= texte.isna() | est_vide(texte.fillna(""))
        parties.append(texte.fillna(""))
    cle = parties[0]
    for p in parties[1:]:
        cle = cle + _SEP + p
    return cle.mask(vide)


def _normale(cle: pd.Series) -> pd.Series:
    """La clé telle qu'un humain la lit : sans espaces de bord, sans casse,
    sans zéros initiaux ni « .0 » final sur une composante numérique."""
    def composante(t: str) -> str:
        t = t.strip().casefold()
        if t.endswith(".0") and t[:-2].isdigit():
            t = t[:-2]
        return (t.lstrip("0") or "0") if t.isdigit() else t

    return cle.map(lambda k: _SEP.join(composante(t) for t in k.split(_SEP)),
                   na_action="ignore")


def _lisible(cle: str) -> str:
    return cle.replace(_SEP, " + ")


def diagnostiquer(gauche: pd.DataFrame, droite: pd.DataFrame,
                  cle_gauche: List[str], cle_droite: List[str],
                  couverture_min: float = 1.0) -> Diagnostic:
    if len(cle_gauche) != len(cle_droite):
        raise CleInconnue("les deux clés n'ont pas le même nombre de colonnes")
    kg, kd = _cle(gauche, cle_gauche, "gauche"), _cle(droite, cle_droite, "droite")
    g, d = kg.dropna(), kd.dropna()

    compte_d = d.value_counts()
    trouve = kg.isin(compte_d.index)                 # une clé vide ne trouve rien
    orphelines = int((~trouve).sum())
    couverture = float(trouve.mean()) if len(kg) else 1.0

    repete_g = bool(g.duplicated().any())
    repete_d = bool(d.duplicated().any())
    cardinalite = {(False, False): "1:1", (True, False): "n:1",
                   (False, True): "1:n", (True, True): "n:m"}[(repete_g, repete_d)]
    lignes_resultat = int(kg.map(compte_d).fillna(0).sum())

    # Presque-correspondances : parmi les orphelines à clé non vide, celles
    # dont la forme normalisée existe à droite.
    orph = kg[~trouve].dropna()
    norm_d = _normale(d)
    table = dict(zip(norm_d, d))                     # forme normale -> une clé d'origine
    norm_orph = _normale(orph)
    rattrapable = norm_orph.isin(table.keys())
    presque = int(rattrapable.sum())
    paires = list(dict.fromkeys(
        (_lisible(a), _lisible(table[n]))
        for a, n in zip(orph[rattrapable], norm_orph[rattrapable])))[:_EXEMPLES]
    ex_orph = [_lisible(k) for k in orph[~rattrapable].drop_duplicates().head(_EXEMPLES)]

    diag = Diagnostic(
        cle_gauche=cle_gauche, cle_droite=cle_droite,
        lignes_gauche=len(gauche), lignes_droite=len(droite),
        cardinalite=cardinalite, couverture=round(couverture, 4),
        couverture_min=couverture_min,
        orphelines=orphelines, exemples_orphelines=ex_orph,
        presque=presque, exemples_presque=paires,
        cles_vides_gauche=int(kg.isna().sum()), cles_vides_droite=int(kd.isna().sum()),
        cles_droite_inutilisees=int((~compte_d.index.isin(g)).sum()),
        lignes_resultat=lignes_resultat,
    )

    if cardinalite == "n:m":
        diag.problemes.append({
            "code": "cardinalite_n_m",
            "message": (f"la clé est répétée des deux côtés (n:m) : la jointure rendrait "
                        f"{lignes_resultat} lignes pour {len(gauche)} à gauche — chaque "
                        f"répétition se multiplie par celles d'en face. Il manque une "
                        f"colonne à la clé, ou l'un des fichiers contient des doublons.")})
    if couverture + 1e-9 < couverture_min:
        m = (f"{orphelines} ligne(s) de gauche sur {len(gauche)} ne trouvent rien à droite "
             f"(couverture {couverture * 100:.1f} %, exigée {couverture_min * 100:.0f} %) : "
             f"une jointure interne les perdrait, une jointure gauche les laisserait vides")
        if diag.cles_vides_gauche:
            m += f" ; {diag.cles_vides_gauche} d'entre elles ont une clé vide"
        if presque:
            m += (f" ; {presque} correspondraient après normalisation (espaces, casse, "
                  f"zéros initiaux), par exemple "
                  + ", ".join(f"{a!r} / {b!r}" for a, b in paires[:3])
                  + " — à décider : est-ce la même clé ?")
        diag.problemes.append({"code": "couverture_insuffisante", "message": m})
    return diag


# --------------------------------------------------------------------------- #
# Ce que la commande et l'API partagent : lire en texte, lire la clé demandée
# --------------------------------------------------------------------------- #
async def lire_en_texte(contenu: bytes, nom: str) -> pd.DataFrame:
    """Le fichier lu en chaînes par le chargeur de la passe sous contrat
    (encodage, séparateur, noms de colonnes) : « 01000 » reste « 01000 »."""
    from app.services.profiler import DataProfiler
    df = await DataProfiler()._load_file(contenu, nom, True)
    if df is None or df.empty:
        raise ValueError(f"{nom} : aucune ligne de données")
    return df


def lire_la_cle(sur: List[str]) -> Tuple[List[str], List[str]]:
    """`a` ou `a=b`, un élément par colonne de clé. Les noms passent par la
    normalisation des en-têtes, pour qu'on puisse écrire « Id Client »."""
    from app.services.profiler import DataProfiler

    def propre(nom: str) -> str:
        return str(DataProfiler._sanitize_columns(pd.Index([nom]))[0])

    g, d = [], []
    for item in (s for s in sur if s and s.strip()):
        a, _, b = item.partition("=")
        g.append(propre(a))
        d.append(propre(b or a))
    if not g:
        raise CleInconnue("aucune colonne de clé n'a été indiquée")
    return g, d
