"""La bannière du dépôt : ce que l'outil refuse, puisque c'est le produit.

    python scripts/make_cover.py

**Pourquoi cette image et pas un logo.** Quelqu'un qui tombe sur le dépôt
décide en deux secondes s'il lit la suite. Un nom en gros ne lui apprend rien.
Ce qui le renseigne, c'est le **mécanisme** — et ici le mécanisme est un
refus : l'outil garde les colonnes qu'il sait traiter honnêtement et écarte
les autres **en disant pourquoi**.

C'est le même geste que la vérification du contrat et que la chaîne de
préparation. Une colonne disparue sans explication est une colonne qu'on
croira avoir utilisée.

**Les colonnes dessinées ne sont pas inventées** : ce sont celles que
`preparation.planifier` rend sur le jeu d'exemple des tests, avec leurs vraies
raisons. Si la logique change, l'image change, parce qu'elle est engendrée et
non dessinée à la main.

Deux formats, parce que GitHub en veut deux :

- `docs/cover.png` (2400x840) — la bannière du README ;
- `docs/social.png` (1280x640) — l'aperçu social, dont GitHub impose la taille.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFont

RACINE = Path(__file__).resolve().parent.parent
SORTIE = RACINE.parent / "docs"

SURFACE = (18, 20, 24)
ENCRE = (255, 255, 255)
ENCRE_2 = (176, 184, 196)
ENCRE_3 = (110, 118, 130)
GARDE = (62, 180, 137)      # vert : ce qui entre dans la chaîne
ECARTE = (214, 92, 92)      # rouge : ce qui en sort, avec sa raison

SANS = "C:/Windows/Fonts/segoeui.ttf"
SANS_G = "C:/Windows/Fonts/segoeuib.ttf"
SANS_SB = "C:/Windows/Fonts/seguisb.ttf"
MONO = "C:/Windows/Fonts/consola.ttf"


def f(chemin: str, taille: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(chemin, taille)


def fond(l: int, h: int) -> Image.Image:
    """Dégradé et halo : un aplat pur se dégrade mal à la recompression que
    GitHub applique aux aperçus."""
    y = np.linspace(0, 1, h, dtype=np.float32)[:, None]
    img = np.repeat(np.repeat(np.array(SURFACE, dtype=np.float32)[None, None, :],
                              h, 0), l, 1)
    img += y[:, :, None] * 10.0
    xx = np.linspace(-1.6, 1.0, l, dtype=np.float32)[None, :]
    yy = np.linspace(-1.2, 1.4, h, dtype=np.float32)[:, None]
    halo = np.exp(-((xx ** 2) * 1.2 + (yy ** 2) * 1.8)) * 26.0
    img += halo[:, :, None] * np.array([0.30, 0.78, 0.62], dtype=np.float32)
    return Image.fromarray(np.clip(img, 0, 255).astype(np.uint8))


def colonnes_reelles() -> list[tuple[str, bool, str]]:
    """(nom, gardée, raison) — rendues par la vraie logique, pas écrites ici."""
    sys.path.insert(0, str(RACINE))
    import numpy as _np
    import pandas as pd

    from app.services.contract import Contrat, ContratColonne
    from app.services.preparation import planifier

    rng = _np.random.default_rng(0)
    n = 300
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "age": rng.integers(18, 80, n).astype(float),
        "revenu": rng.normal(2500, 600, n).round(2),
        "ville": rng.choice(["Casablanca", "Rabat", "Fes", "Tanger"], n),
        "code": [f"REF-{i:05d}" for i in range(n)],
        "inscrit_le": pd.date_range("2024-01-01", periods=n).astype(str),
        "churn": rng.choice([0, 1], n),
    })
    contrat = Contrat(
        colonnes=[ContratColonne("id", "integer", unique=True),
                  ContratColonne("age", "integer"),
                  ContratColonne("revenu", "number"),
                  ContratColonne("ville", "string"),
                  ContratColonne("code", "string"),
                  ContratColonne("inscrit_le", "datetime"),
                  ContratColonne("churn", "integer")],
        cle_primaire=["id"])
    plan = planifier(df, contrat, cible="churn")

    courtes = {
        "id": "clé primaire",
        "code": "300 modalités",
        "inscrit_le": "date non dérivée",
    }
    sortie: list[tuple[str, bool, str]] = []
    for nom in plan.numeriques:
        sortie.append((nom, True, "numérique"))
    for nom in plan.categorielles:
        sortie.append((nom, True, "catégorielle"))
    for role in plan.ecartees:
        sortie.append((role.nom, False, courtes.get(role.nom, "écartée")))
    return sortie


def nombre_de_tests() -> int:
    """Le vrai nombre, demandé à pytest.

    L'image affichait « 151 tests » alors qu'il y en avait 185. Le reste de ce
    fichier dessine à partir de la logique réelle, précisément pour que
    l'image ne mente pas quand le code change ; un nombre écrit à la main au
    milieu de ça est le seul endroit par où le mensonge pouvait rentrer.
    """
    sortie = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q",
         "-p", "no:cacheprovider"],
        cwd=RACINE, capture_output=True, text=True, encoding="utf-8",
        errors="replace",
    ).stdout
    trouve = re.search(r"(\d+)\s+tests? collected", sortie)
    if not trouve:
        raise SystemExit(
            "impossible de compter les tests ; plutôt que d'afficher un "
            f"nombre faux, on s'arrête.\n{sortie[-600:]}"
        )
    return int(trouve.group(1))


def composer(l: int, h: int, e: float, tests: int) -> Image.Image:
    img = fond(l, h)
    d = ImageDraw.Draw(img)
    marge = int(l * 0.062)

    d.rectangle([marge, int(h * 0.145), marge + int(96 * e), int(h * 0.145) + int(5 * e)],
                fill=GARDE)
    d.text((marge, int(h * 0.19)), "Intelligent Data Cleaner",
           font=f(SANS_G, int(86 * e)), fill=ENCRE)
    d.text((marge, int(h * 0.355)),
           "Le prétraitement en entier — et ce qu'il refuse de faire, dit en clair.",
           font=f(SANS, int(38 * e)), fill=ENCRE_2)

    # Les colonnes : gardées à gauche, écartées à droite, chacune avec son mot.
    y = int(h * 0.55)
    x = marge
    police = f(SANS_SB, int(30 * e))
    petite = f(MONO, int(23 * e))
    for nom, gardee, raison in colonnes_reelles():
        teinte = GARDE if gardee else ECARTE
        largeur = max(d.textlength(nom, font=police),
                      d.textlength(raison, font=petite)) + int(46 * e)
        hauteur = int(96 * e)
        if x + largeur > l - marge:          # on passe à la ligne
            x = marge
            y += hauteur + int(20 * e)
        d.rounded_rectangle([x, y, x + largeur, y + hauteur],
                            radius=int(12 * e),
                            fill=(*teinte, 24) if False else None,
                            outline=teinte, width=max(2, int(2 * e)))
        d.text((x + int(22 * e), y + int(16 * e)), nom, font=police,
               fill=ENCRE if gardee else (235, 200, 200))
        d.text((x + int(22 * e), y + int(56 * e)), raison, font=petite,
               fill=ENCRE_3 if gardee else teinte)
        if not gardee:                        # la barre : la forme, pas la couleur
            d.line([(x + int(16 * e), y + int(30 * e)),
                    (x + largeur - int(16 * e), y + int(30 * e))],
                   fill=teinte, width=max(2, int(2 * e)))
        x += largeur + int(18 * e)

    d.text((marge, h - int(h * 0.105)),
           "Un contrat vérifié indépendamment du nettoyeur  ·  une chaîne "
           f"ajustée après la séparation  ·  {tests} tests",
           font=f(SANS, int(26 * e)), fill=ENCRE_3)
    return img


def main() -> int:
    SORTIE.mkdir(parents=True, exist_ok=True)
    tests = nombre_de_tests()
    for nom, (l, h, e) in {"cover.png": (2400, 840, 1.0),
                           "social.png": (1280, 640, 0.62)}.items():
        chemin = SORTIE / nom
        composer(l, h, e, tests).save(chemin, optimize=True)
        print(f"ecrit : {chemin.name}  {l}x{h}  "
              f"({chemin.stat().st_size / 1024:.0f} Ko)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
