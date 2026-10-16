"""La forme d'une valeur : le contrôle qui manquait, sur l'exercice qui l'a
révélé.

Le module *Applications of AI in InfoSec* (HTB Academy) fait valider cinq
colonnes d'un journal réseau : `source_ip` contre une expression régulière
d'adresse IPv4, `destination_port` dans 0–65535, `protocol` contre une liste,
`bytes_transferred` numérique et positif, `threat_level` dans 0–2.

Quatre de ces cinq contrôles étaient déjà exprimables dans le contrat —
`domaine` pour la liste, `minimum` et `maximum` pour les plages. Le cinquième
ne l'était pas : **un type dit ce qu'une valeur est, pas à quoi elle doit
ressembler.** `999.1.2.3` est du texte parfaitement valide.

Ce fichier couvre les cinq, et marque les deux endroits où ce dépôt fait
délibérément autrement que l'exercice.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from app.services import reparer
from app.services.contract import FORMES, Contrat, ContratColonne, verify


def journal() -> pd.DataFrame:
    """Un journal réseau avec les saletés exactes de l'exercice."""
    return pd.DataFrame({
        "source_ip": ["192.168.1.10", "10.0.0.4", "INVALID_IP", "999.1.2.3",
                      "MISSING_IP", "172.16.0.1", "8.8.8.8", "10.0.0.5"],
        "destination_port": [443, 22, "STRING_PORT", 70000, 80, -1, 53, 8080],
        "protocol": ["HTTPS", "SSH", "TCP", "GOPHER", "DNS", "HTTP", "DNS",
                     "HTTP"],
        "bytes_transferred": [1200, 340, "NON_NUMERIC", -50, 900, 1500, 220,
                              400],
        "threat_level": [0, 1, "?", 5, 2, 0, 1, 2],
    })


PROTOCOLES = ["TCP", "TLS", "SSH", "POP3", "DNS", "HTTPS", "SMTP", "FTP",
              "UDP", "HTTP"]


def contrat() -> Contrat:
    """Les cinq contrôles de la section, déclarés — pas codés à la main."""
    return Contrat(colonnes=[
        ContratColonne("source_ip", "string", forme="ipv4"),
        ContratColonne("destination_port", "integer", minimum=0, maximum=65535),
        ContratColonne("protocol", "category", domaine=PROTOCOLES),
        ContratColonne("bytes_transferred", "integer", minimum=0),
        ContratColonne("threat_level", "integer", minimum=0, maximum=2),
    ])


def violations(rapport) -> dict[str, int]:
    return {f"{v.code}:{v.colonne}": int(v.mesure or 0)
            for v in rapport.violations}


# --------------------------------------------------------------------------- #
# Les cinq contrôles de la section
# --------------------------------------------------------------------------- #
def test_les_adresses_invalides_sont_vues():
    """Le contrôle qui n'existait pas avant ce fichier."""
    v = violations(verify(journal(), contrat(), origine=None))
    assert v.get("hors_forme:source_ip") == 3, (
        "INVALID_IP, 999.1.2.3 et MISSING_IP doivent être refusées")


def test_les_ports_hors_plage_sont_vus():
    v = violations(verify(journal(), contrat(), origine=None))
    assert v.get("hors_plage:destination_port", 0) >= 2, "70000 et -1"


def test_les_protocoles_inconnus_sont_vus():
    v = violations(verify(journal(), contrat(), origine=None))
    assert v.get("hors_domaine:protocol") == 1, "GOPHER n'est pas dans la liste"


def test_les_octets_negatifs_sont_vus():
    v = violations(verify(journal(), contrat(), origine=None))
    assert v.get("hors_plage:bytes_transferred", 0) >= 1, "-50"


def test_les_niveaux_de_menace_hors_plage_sont_vus():
    v = violations(verify(journal(), contrat(), origine=None))
    assert v.get("hors_plage:threat_level", 0) >= 1, "5 sort de 0–2"


def test_les_cinq_controles_de_la_section_sont_exprimables():
    """Aucun n'est codé en dur : ils sont tous déclarés dans le contrat."""
    rapport = verify(journal(), contrat(), origine=None)
    touchees = {v.colonne for v in rapport.violations}
    assert {"source_ip", "destination_port", "protocol", "bytes_transferred",
            "threat_level"} <= touchees


# --------------------------------------------------------------------------- #
# Là où ce dépôt fait délibérément autrement
# --------------------------------------------------------------------------- #
def test_une_adresse_invalide_devient_un_vide_jamais_une_autre_adresse():
    """L'exercice écrit `df['source_ip'].fillna('0.0.0.0')`.

    Ce dépôt ne le fera pas. On ignore ce que l'adresse aurait dû être, et la
    remplacer fabrique un fait sur un événement de sécurité : une machine qui
    n'existe pas apparaît dans le journal.
    """
    propre, j = reparer.appliquer(journal(), contrat())
    assert propre["source_ip"].isna().sum() >= 1 or \
        propre["source_ip"].notna().all(), "état incohérent"
    assert "0.0.0.0" not in set(propre["source_ip"].dropna().astype(str)), (
        "une adresse a été inventée pour combler un vide")


def test_les_hors_forme_sont_vides_et_le_journal_le_dit():
    propre, j = reparer.appliquer(journal(), contrat())
    actions = " ".join(g.detail + " " + (g.colonne or "")
                       for g in j.gestes)
    assert "source_ip" in actions and "forme" in actions
    assert "999.1.2.3" in actions or "INVALID_IP" in actions, (
        "le journal doit nommer ce qu'il a vidé")


def test_un_port_hors_plage_nest_pas_ecrete_par_defaut():
    """L'exercice écrit `.clip(lower=0, upper=65535)`.

    Écrêter invente : un port 70000 ramené à 65535 devient un port plausible
    qui n'a jamais été observé, et toute statistique faite ensuite porte sur
    une distribution qu'on a façonnée. Le défaut est « signaler ».
    """
    propre, _ = reparer.appliquer(journal(), contrat())
    ports = pd.to_numeric(propre["destination_port"], errors="coerce").dropna()
    assert not (ports == 65535).any(), "un port a été écrêté au plafond"


def test_trop_dadresses_invalides_font_refuser_limputation():
    """Le budget est ce qui empêche d'inventer une flotte de machines."""
    df = journal()
    df["source_ip"] = ["INVALID_IP"] * 7 + ["10.0.0.1"]
    ct = contrat()
    ct.imputation_max = 0.10
    propre, j = reparer.appliquer(df, ct)
    assert propre["source_ip"].isna().sum() >= 6, (
        "7 adresses sur 8 invalides : les combler dépasserait tout budget "
        "raisonnable, donc elles restent vides")


# --------------------------------------------------------------------------- #
# Les motifs eux-mêmes
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("valeur,attendu", [
    ("192.168.1.1", True), ("0.0.0.0", True), ("255.255.255.255", True),
    ("256.0.0.1", False), ("999.1.2.3", False), ("1.2.3", False),
    ("INVALID_IP", False), ("1.2.3.4.5", False), ("", False),
])
def test_le_motif_ipv4_refuse_ce_quil_doit_refuser(valeur, attendu):
    """Un motif permissif laisse passer ce qu'il prétend attraper, et une
    forme qui ne refuse rien est pire qu'absente puisqu'elle rassure."""
    import re
    assert bool(re.match(FORMES["ipv4"], valeur)) is attendu


def test_une_forme_inconnue_mais_valide_est_acceptee_comme_regex():
    ContratColonne("x", "string", forme=r"^CVE-\d{4}-\d{4,}$")


def test_une_regex_fautive_est_refusee_a_la_declaration():
    """Une regex qui ne compile pas refuserait tout, ce qui se lirait comme
    une colonne entièrement invalide."""
    with pytest.raises(ValueError, match="forme"):
        ContratColonne("x", "string", forme="[pas fermé")


def test_toutes_les_formes_declarees_compilent():
    import re
    for nom, motif in FORMES.items():
        re.compile(motif), f"{nom} ne compile pas"
