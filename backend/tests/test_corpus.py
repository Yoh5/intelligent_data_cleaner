"""Le corpus de pathologies, exécuté comme suite de non-régression.

Chaque cas de `corpus.py` passe la passe complète et vérifie son attente. Ils
sont paramétrés un par un plutôt que lancés en bloc : quand un casse, c'est son
nom qui s'affiche, pas « le corpus ».

Ce que ce fichier a trouvé dès la première exécution, et qui est maintenant
verrouillé ici :

- **les zéros initiaux étaient mangés.** `01000` devenait `1000`, et aucun
  contrôle de type ne pouvait le voir puisque la colonne tenait parfaitement son
  type. La cause était à la lecture : pandas convertit avant que quoi que ce
  soit ne s'exécute. La passe lit désormais tout en texte et décide elle-même ;
- **un fichier sans ligne de données plantait** en erreur 500 au lieu de rendre
  un message ;
- **les négatifs comptables `(1200)`** n'étaient pas signalés.
"""
import asyncio

import pytest
from corpus import corpus

from app.services import agent_contrat

CAS = corpus()


@pytest.mark.parametrize("cas", CAS, ids=[c.nom for c in CAS])
def test_le_corpus_passe(cas):
    if cas.nom == "entete-seul.csv":
        pytest.skip("couvert par test_un_fichier_sans_donnees_rend_un_message")
    resultat = asyncio.run(agent_contrat.run(cas.octets, cas.nom, usage="modele"))
    cas.attendu(resultat)


def test_un_fichier_sans_donnees_rend_un_message_pas_une_panne():
    """Un en-tête seul est normal avant un premier chargement et grave après.

    L'outil ne peut pas trancher, donc il le dit — et le dit en 400, pas en 500 :
    une erreur 500 ferait croire à une panne de notre côté et enverrait
    l'utilisateur chercher au mauvais endroit.
    """
    from fastapi.testclient import TestClient

    from app.main import app
    r = TestClient(app).post(
        "/contract-clean/",
        files={"file": ("vide.csv", b"id,valeur\n", "text/csv")})
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "aucune ligne de données" in detail
    assert "trancher" in detail


def test_le_corpus_couvre_ce_quon_croit():
    """Un corpus qui rétrécit sans qu'on le remarque ne garde plus rien."""
    noms = {c.nom for c in CAS}
    for attendu in ("zeros-en-tete.csv", "geoid.csv", "comptable.csv",
                    "entete-seul.csv", "nuls-multilingues.csv"):
        assert attendu in noms, attendu
    assert len(CAS) >= 20, len(CAS)
    assert all(c.enjeu for c in CAS), "chaque cas doit dire ce qu'il protège"
