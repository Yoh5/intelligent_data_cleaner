# Corpus public — des fichiers que personne n'a rangés pour nous

`tests/corpus.py` couvre les pathologies qu'on **connaît** : une par fichier,
fabriquée, avec son attente. Ce répertoire couvre l'autre moitié — des jeux
réels, désordonnés d'une façon qu'on n'invente pas.

Les deux sont nécessaires et ne trouvent pas les mêmes choses. Le corpus
fabriqué a trouvé que les zéros initiaux étaient mangés ; celui-ci a trouvé que
le chargeur ne reconnaît pas `?` comme marqueur de vide.

## Les fichiers

| fichier | origine | ce qu'il éprouve |
|---|---|---|
| `uci-adult.csv` | UCI Machine Learning Repository | **`?` comme marqueur de vide** sur `workclass`, `occupation`, `native-country` — 5,6 % de la colonne. La liste du chargeur connaît `???` mais pas `?` |
| `titanic.csv` | Data Science Dojo | `age` vide à 19,9 %, `cabin` à 77,1 % — le cas d'école du budget d'imputation. Et deux familles de valeurs extrêmes **authentiques** |
| `us-cities.csv` | plotly/datasets | 3 228 lignes, noms de villes et coordonnées |
| `owid-vaccinations.csv` | Our World in Data | colonnes de texte libre, sources citées, dates |
| `airtravel.csv` | FSU (Burkardt) | douze lignes, en-têtes avec espaces, nombres à virgule de milliers |

## Ce que Titanic démontre, et qui vaut plus que le reste

La passe y signale comme hors de l'intervalle plausible :

- **`fare` = 512,33 £**, trois fois — la famille Cardeza, les billets les plus
  chers du navire ;
- **`sibsp` = 8** — la famille Sage, onze membres, tous disparus.

**Ces valeurs sont vraies.** Un outil qui écrête « les aberrantes » par défaut
détruit ici les lignes les plus intéressantes du jeu de données le plus enseigné
du métier — et le fait en silence, en laissant croire qu'il a nettoyé. C'est
exactement pourquoi `politique_aberrantes` vaut `"signaler"` par défaut : une
valeur extrême est tantôt une erreur de saisie, tantôt le cas qui porte toute
l'analyse, et la colonne seule ne permet pas de les distinguer.

## Journaux de sécurité — `securite/`

Quatre jeux réels, non versionnés eux non plus :

| fichier | origine | ce qu'il a trouvé |
|---|---|---|
| `openssh.csv` | Loghub, `OpenSSH/OpenSSH_2k.log_structured.csv` | **une date inventée** : la colonne `Time` (`06:55:46`) sortait en `2026-11-15 06:55:46`, la date du jour de l'exécution |
| `apache.csv`, `linux.csv` | Loghub, `Apache/` et `Linux/`, mêmes fichiers `_2k.log_structured.csv` | rien de plus — ils servent de témoins |
| `zeek-conn.log.gz` | brimdata/zed-sample-data, `zeek-default/conn.log.gz` (1 021 951 connexions) | le format natif de Zeek, lu depuis le 15/11 : 122 Mo décompressés, contrat tenu en 87 s. A montré que la forme IPv6 rejetait `fe80::215:17ff:fe84:c13f` |
| `zeek-conn-20k.csv` | les 20 000 premières lignes du précédent, converties en CSV | **des codes détruits** : `history` passait de 119 à 102 valeurs, fusionnées « par la casse » alors que la majuscule y désigne l'initiateur du paquet |

Loghub : `https://raw.githubusercontent.com/logpai/loghub/master/<Jeu>/<Jeu>_2k.log_structured.csv`.
Zeek : `https://github.com/brimdata/zed-sample-data/raw/main/zeek-default/conn.log.gz`.

Dans les deux cas le contrat était déclaré **tenu, 100/100**. Ce sont ces
fichiers qui ont montré que « ne rien inventer » n'était pas encore vrai pour
les dates, ni « ne rien détruire » pour la casse.

## Ces fichiers sont des données, pas du code

Ils viennent du réseau. Ils vivent donc dans leur propre répertoire, à l'écart
des sources, et rien n'est jamais exécuté depuis ici : les scripts qui les
lisent sont ailleurs et reçoivent les chemins en arguments. Un interpréteur
lancé depuis un répertoire de données charge les modules qui s'y trouvent, et un
fichier nommé `json.py` déposé là s'exécuterait au premier `import json`.

## Les rejouer

```bash
cd backend
python -m pytest tests/test_corpus_public.py -q   # ignoré si le répertoire est vide
```

Le test est volontairement tolérant à l'absence des fichiers : ils ne sont pas
versionnés, pour ne pas redistribuer des jeux dont les licences diffèrent et
pour ne pas gonfler le dépôt de 3,5 Mo. Les URL sont dans ce fichier, et
retélécharger prend trente secondes.

| fichier | URL |
|---|---|
| `uci-adult.csv` | `https://archive.ics.uci.edu/static/public/2/data.csv` |
| `titanic.csv` | `https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv` |
| `us-cities.csv` | `https://raw.githubusercontent.com/plotly/datasets/master/2014_us_cities.csv` |
| `owid-vaccinations.csv` | `https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/locations.csv` |
| `airtravel.csv` | `https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv` |
