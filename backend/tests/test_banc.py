"""Le banc de mesure, rejoué à chaque push sur une petite table synthétique.

Il ne vérifie pas que le banc « passe » : il fige ce qu'il a mesuré le
16/11/2026, pour qu'une régression se voie le jour même. Le jeu synthétique
est entièrement fabriqué, donc identique sur la CI et en local (Titanic n'est
pas versionné).
"""
import pytest

from bench import concurrents, donnees
from bench.defauts import abimer
from bench.mesure import canonique, mesurer


@pytest.fixture(scope="module")
def echantillon():
    return abimer(donnees.synthetique(n=400, graine=3), graine=103)


def _passer(ech, nom):
    (sortie, verdict, detail), _, err = concurrents.executer(nom, ech.octets, "banc.csv")
    assert not err, err
    m = mesurer(ech, sortie, nom, concurrents.PROFILS.get(nom, "base"))
    m.verdict_outil = verdict
    return m


@pytest.mark.parametrize("nom", ["contrat_base", "contrat_modele"])
def test_la_passe_sous_contrat_rend_une_sortie_prete_et_le_dit(echantillon, nom):
    m = _passer(echantillon, nom)
    assert m.defauts > 50
    assert m.corrigees == m.defauts, m.exemples_manques
    assert m.cassees == 0, m.exemples_casses
    assert m.inventees_silencieuses == 0
    assert m.doublons_restants == 0 and m.lignes_perdues == 0
    assert m.prete and m.verdict_juste


def test_lusage_modele_comble_mais_le_declare(echantillon):
    m = _passer(echantillon, "contrat_modele")
    assert m.inventees_declarees > 0 and m.inventees_silencieuses == 0


def test_lancienne_boucle_se_donne_un_satisfecit_que_le_banc_refuse(echantillon):
    # Le constat fondateur du projet, désormais mesuré : elle se juge prête,
    # elle ne l'est pas — des codes client amputés de leur zéro initial.
    m = _passer(echantillon, "ancienne_boucle")
    assert m.verdict_outil is True
    assert not m.prete and m.verdict_juste is False
    assert m.cassees > 0
    assert any("code_client" in e for e in m.exemples_casses + m.exemples_manques)


def test_le_carnet_pandas_mange_les_zeros_initiaux(echantillon):
    m = _passer(echantillon, "carnet_pandas")
    assert any("code_client" in e for e in m.exemples_casses)


# --------------------------------------------------------------------------- #
# Le juge lui-même : une définition de « prêt » qui se trompe fausse tout
# --------------------------------------------------------------------------- #
def test_la_forme_prete_est_stricte():
    assert canonique("1234.5", "nombre") == repr(1234.5)
    assert canonique("1234,5", "nombre").startswith("\x00")
    assert canonique("42", "entier") == "42" and canonique("42.0", "entier").startswith("\x00")
    assert canonique("2026-03-15", "date") == "2026-03-15"
    assert canonique("2026-03-15 00:00:00", "date") == "2026-03-15"
    assert canonique("15/03/2026", "date").startswith("\x00")
    assert canonique("", "texte") is None and canonique("N/A", "texte") == "N/A"


def test_le_banc_attrape_un_nettoyeur_qui_ne_fait_rien(echantillon):
    # Un nettoyeur qui rend le fichier tel quel doit tout manquer : sinon le
    # banc ne mesure rien.
    m = mesurer(echantillon, echantillon.octets, "rien")
    assert m.corrigees < m.defauts * 0.5 and m.doublons_restants == echantillon.doublons
    assert not m.prete
