# Le banc de mesure

**La question :** après exécution, les données sont-elles réellement prêtes à
l'usage ? Et quand un nettoyeur dit « prêt », a-t-il raison ?

On ne peut pas y répondre sur un fichier réel : personne ne connaît la bonne
valeur de chaque cellule. Le banc la fabrique. Il part d'une table dont chaque
cellule est connue, y injecte des défauts nommés et localisés, passe le fichier
dans un nettoyeur, puis compare chaque cellule livrée à la vérité.

```bash
cd backend
python -m bench.run                          # deux jeux, quatre nettoyeurs
python -m bench.run --graines 7 8 9 --json resultats.json
pytest tests/test_banc.py                    # rejoué en CI sur une table synthétique
```

## Ce qui est mesuré

| mesure | définition |
|---|---|
| défauts corrigés | cellules abîmées rendues à leur vraie valeur |
| cellules saines **cassées** | cellules intactes que le nettoyage a abîmées — le dégât que personne ne voit |
| inventées silencieuses / déclarées | une valeur posée là où la vérité est « inconnu » ; *déclarée* si une colonne `<col>_impute` la signale |
| doublons restants, lignes perdues | au niveau de la ligne |
| **verdict juste ?** | le nettoyeur dit « prêt » : le banc est-il d'accord ? |

« Prêt à l'usage » est défini une fois, dans `mesure.py`, et sévèrement : un
nombre a un point décimal, un entier pas de `.0`, une date est ISO, un code
garde ses zéros, l'inconnu est une cellule vide. Pour un chargement en base
rien ne doit être inventé ; pour un modèle, combler est permis **si c'est déclaré**.

## Les défauts injectés

Marqueurs de vide (`N/A`, `inconnu`, `?`…), espaces de bord, casse (`rabat`,
`RABAT`), virgule décimale, dates `jj/mm/aaaa` au milieu de dates ISO, lignes
recopiées. Et des **pièges jamais abîmés** : codes à zéro initial, identifiants,
booléens, valeurs extrêmes vraies. Une différence sur eux ne peut venir que du
nettoyeur.

## Résultats du 16/11/2026 (graines 7, 8, 9 — identiques à une cellule près)

Jeu synthétique, 1 000 lignes + 30 doublons, 261 défauts :

| nettoyeur | corrigés | saines cassées | inventées silencieuses | prête ? | se dit prêt ? |
|---|---|---|---|---|---|
| passe sous contrat, usage base | **100 %** | **0** | 0 | oui | oui — juste |
| passe sous contrat, usage modèle | **100 %** | **0** | 0 (50 déclarées) | oui | oui — juste |
| ancienne boucle (`/agent-clean`) | 59 % | 1 058 | 0 | non | **oui — faux** |
| carnet pandas en dix minutes | 44 % | 108 | 36 | non | ne dit rien |

Titanic (714 lignes complètes, distributions réelles), 203 défauts :

| nettoyeur | corrigés | saines cassées | inventées silencieuses | prête ? | se dit prêt ? |
|---|---|---|---|---|---|
| passe sous contrat, usage base | **100 %** | **0** | 0 | oui | oui — juste |
| passe sous contrat, usage modèle | **100 %** | **0** | 0 (35 déclarées) | oui | oui — juste |
| ancienne boucle | 80 % | 18 | 35 | non | **oui — faux** |
| carnet pandas | 68 % | 18 | 25 | non | ne dit rien |

**Lire les 1 058 cellules cassées de l'ancienne boucle honnêtement :** 950 sont
des entiers réécrits `75.0` — une colonne INTEGER les refuserait, mais c'est un
défaut de forme. Le dégât de fond : **83 codes client amputés de leur zéro
initial** et 25 âges inventés.

## Ce que le banc a trouvé dans la passe sous contrat — corrigé

1. **Les dates mélangées étaient abandonnées.** Une colonne à 95 % ISO et 5 %
   `jj/mm/aaaa` sortait en texte, contrat « tenu, 100/100 », et les 40 dates
   françaises restaient illisibles pour une base. Cause : pandas déduit le
   format de la première valeur et met les autres à vide. `contract.lire_dates`
   lit maintenant chaque format à part.
2. **Une imputation sous 5 % était silencieuse.** L'indicatrice n'était posée
   qu'au-delà de 5 % ; sur Titanic, 35 âges (4,9 %) étaient comblés sans trace
   et le contrat se disait tenu. Elle est posée dès la première valeur comblée.

Avant correction, le verdict de la passe était **faux deux fois sur quatre**.

## Limites — à lire avant de citer ces chiffres

- **J'ai écrit l'outil ET le banc.** Le 100 % de la passe sous contrat veut dire
  « aucun des défauts que j'ai pensé à injecter ne lui résiste », pas « elle
  nettoie tout ». Le banc devient utile à mesure que ses défauts dépassent ce
  que l'outil sait déjà faire.
- **Six familles de défauts seulement.** Manquent : séparateurs de milliers,
  négatifs comptables `(1200)`, fautes de frappe dans les catégories (`Casblanca`),
  quasi-doublons, unités mélangées, extrêmes qui SONT des fautes, encodages.
- **Deux jeux.** Un synthétique, un réel (Titanic, non versionné).
- **Pas encore de comparaison avec Great Expectations, Pandera ou Soda.** Ce
  sont des vérificateurs plutôt que des nettoyeurs : la comparaison juste porte
  sur la DÉTECTION des défauts, à ajouter.
