"""Le banc de mesure : des défauts injectés dont on connaît la vérité.

Le vérificateur de la passe sous contrat est indépendant du nettoyeur, mais il
reste un jugement de l'outil sur sa propre sortie. Ce banc répond à une autre
question, de l'extérieur : **après exécution, les données sont-elles réellement
prêtes à l'usage ?** On part d'une table dont chaque cellule est connue, on y
injecte des défauts nommés, on passe le fichier dans un nettoyeur, puis on
compare chaque cellule livrée à la vérité.

Quatre mesures, et c'est la troisième qui compte le plus :

- **corrigées** — cellules défectueuses rendues à leur vraie valeur ;
- **manquées** — cellules défectueuses restées fausses ;
- **cassées** — cellules SAINES abîmées par le nettoyage (un zéro initial mangé,
  une valeur extrême écrêtée) : le dégât que personne ne voit ;
- **inventées** — une valeur posée là où la vérité est « inconnu ».

Et une cinquième, qui répond à l'idée d'auto-évaluation : **le verdict de
l'outil sur lui-même est-il juste ?** Un outil qui dit « prêt » sur une sortie
que le banc juge non prête ment, et c'est précisément ce que la première
version faisait.

Rien ici n'appelle de modèle de langage : le banc est gratuit et reproductible
à la graine près.
"""
