"""Ce que le serveur accepte d'exécuter, et avec quoi il l'exécute.

`POST /execute/` lance un script Python fourni par le client. C'est le produit :
l'utilisateur relit le script généré, le corrige, puis l'exécute. Le problème
n'était donc pas qu'on exécute du code — c'est qu'on l'exécutait avec
l'environnement du serveur, `OPENAI_API_KEY` comprise, et qu'on renvoyait
`stdout` au client. Trois lignes ressortaient la clé.

Deux barrières, dans cet ordre d'importance :

1. **L'environnement du sous-processus est construit, jamais hérité.** C'est la
   vraie. Un processus qui ne détient aucun secret n'en fuite aucun.
2. **Le script doit ressembler à ce que le générateur produit** : une liste
   blanche d'imports, et un refus des quelques détours connus pour la
   contourner (`__import__`, `eval`, `exec`, la remontée par `__subclasses__`).

La seconde est de la défense en profondeur, **pas un bac à sable**. Du Python
arbitraire reste du Python arbitraire : qui veut vraiment sortir d'une liste
blanche d'imports y arrive. Prétendre le contraire serait plus dangereux que de
ne rien faire, parce que quelqu'un finirait par exposer ce service en se
croyant protégé. Ce qui protège ici, c'est le couple environnement vide +
répertoire temporaire + délai de 30 s ; la liste blanche ne fait qu'arrêter
l'accident et le curieux.
"""

from __future__ import annotations

import ast
import os

#: Ce que le générateur écrit en tête de chaque script, et rien de plus.
#: Toute entrée ajoutée ici est une décision de sécurité : `os` donnerait
#: l'environnement, `socket`/`urllib`/`requests` donneraient la sortie réseau,
#: `subprocess` donnerait tout le reste.
ALLOWED_IMPORTS = frozenset({
    "pandas", "numpy", "pathlib", "logging", "sys", "argparse", "typing",
    "warnings", "re", "math", "datetime", "json", "csv", "decimal", "collections",
})

#: Les noms qui rouvrent la porte que la liste blanche vient de fermer.
FORBIDDEN_NAMES = frozenset({
    "__import__", "eval", "exec", "compile", "globals", "locals", "vars",
    # `getattr(__builtins__, "__import__")` contourne la liste ci-dessus en
    # n'écrivant jamais le nom interdit : c'est une chaîne, pas un identifiant.
    # On refuse donc l'accès dynamique aux attributs, dont ce script n'a
    # aucun besoin, plutôt que de courir après les chaînes.
    "getattr", "setattr", "delattr", "__builtins__", "breakpoint", "memoryview",
})

#: Les attributs par lesquels on remonte du bac à sable vers l'interpréteur.
FORBIDDEN_ATTRIBUTES = frozenset({
    "__subclasses__", "__bases__", "__mro__", "__globals__", "__builtins__",
    "__code__", "__class__", "__dict__", "__getattribute__", "__reduce__",
})


class ScriptRefused(ValueError):
    """Le script n'a pas la forme de ce que cette application génère."""


def _root(module: str) -> str:
    return module.split(".", 1)[0]


def check_script(script: str) -> None:
    """Accepte le script, ou dit précisément ce qui l'a fait refuser."""
    try:
        tree = ast.parse(script)
    except SyntaxError as error:
        raise ScriptRefused(f"erreur de syntaxe ligne {error.lineno} : {error.msg}") from error

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _root(alias.name) not in ALLOWED_IMPORTS:
                    raise ScriptRefused(
                        f"import refusé ligne {node.lineno} : {alias.name}. "
                        "Un script de nettoyage lit un fichier et écrit un fichier ; "
                        "il n'a besoin de rien d'autre."
                    )

        elif isinstance(node, ast.ImportFrom):
            # `from . import x` n'a pas de module : il n'y a pas de paquet ici.
            module = node.module or ""
            if node.level or _root(module) not in ALLOWED_IMPORTS:
                raise ScriptRefused(
                    f"import refusé ligne {node.lineno} : {module or 'relatif'}. "
                    "Un script de nettoyage lit un fichier et écrit un fichier ; "
                    "il n'a besoin de rien d'autre."
                )

        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            raise ScriptRefused(
                f"appel refusé ligne {node.lineno} : {node.id}. "
                "Ces fonctions rouvrent la porte que la liste d'imports ferme."
            )

        elif isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_ATTRIBUTES:
            raise ScriptRefused(
                f"attribut refusé ligne {node.lineno} : {node.attr}. "
                "C'est le chemin classique pour remonter vers l'interpréteur."
            )


def subprocess_env(parent: dict[str, str] | None = None) -> dict[str, str]:
    """L'environnement du script : construit, jamais hérité.

    La barrière qui compte. Le script généré n'a jamais eu besoin d'une
    variable du serveur, et le serveur en détient une qui vaut de l'argent.
    On ne garde donc que ce sans quoi l'interpréteur ne démarre pas.
    """
    parent = os.environ if parent is None else parent

    env = {"PYTHONIOENCODING": "utf-8", "PYTHONDONTWRITEBYTECODE": "1"}

    # Le strict nécessaire pour qu'un interpréteur Python démarre ET retrouve
    # ses propres bibliothèques. APPDATA et HOME sont dans la liste parce que
    # Python y cherche le *user site* (%APPDATA%\Python\PythonXY\site-packages,
    # ~/.local/lib) : sans eux, `import pandas` échoue alors que pandas est
    # installé. Un test lance un vrai script généré pour que ce durcissement ne
    # puisse plus casser le produit en silence.
    #
    # Ce sont des chemins, pas des secrets. Et l'environnement n'est pas une
    # frontière de système de fichiers : un script peut lire ce répertoire en
    # écrivant le chemin en dur. Ce qu'on ferme ici, c'est la fuite de ce que
    # le serveur détient en mémoire, pas l'accès au disque.
    for name in ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "APPDATA", "LOCALAPPDATA",
                 "HOME", "USERPROFILE", "TEMP", "TMP", "LANG", "LC_ALL"):
        value = parent.get(name) or parent.get(name.lower())
        if value:
            env[name] = value

    return env
