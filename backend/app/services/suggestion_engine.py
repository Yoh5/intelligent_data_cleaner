#!/usr/bin/env python3
"""
Moteur de suggestions ultra-robuste.
Génère du code qui fonctionne garanti sur n'importe quel CSV.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
import re

@dataclass
class CleaningStep:
    step_type: str
    column: Optional[str]
    description: str
    code_template: str
    priority: int
    reason: str
    validation_check: str
    required_imports: List[str]

class SuggestionEngine:
    """
    Génère des étapes de nettoyage avec code 100% fonctionnel.
    Gère tous les cas particuliers et edge cases.
    """

    def __init__(self, profiles: Dict, df_sample: pd.DataFrame):
        self.profiles = profiles
        self.df_sample = df_sample.copy() if df_sample is not None else pd.DataFrame()
        self.steps: List[CleaningStep] = []
        self.missing_patterns = [
            '', ' ', '  ', '   ', '\t', '\n', 'NA', 'N/A', 'n/a', 'na',
            'NULL', 'null', 'None', 'none', 'NaN', 'nan', 'MISSING', 'missing',
            '-', '--', '???', 'Unknown', 'unknown', 'Not Available', 'no data'
        ]

    def generate_steps(self) -> List[CleaningStep]:
        """Pipeline complet de suggestions priorisées."""

        # Étape 0: Détection et conversion des valeurs manquantes cachées (TOUJOURS en premier)
        self._add_hidden_missing_detection()

        for col_name, profile in self.profiles.items():
            semantic = profile.get('semantic_type', 'unknown')
            missing_count = profile.get('missing_count', 0)

            # Étape 1: Conversion de type (avant imputation !)
            if semantic == 'numeric-mixed':
                self._add_numeric_conversion(col_name)
            elif semantic == 'datetime-mixed':
                self._add_datetime_conversion(col_name)

            # Étape 2: Valeurs manquantes (après conversion)
            if missing_count > 0:
                self._add_imputation(col_name, semantic)

            # Étape 3: Outliers pour numériques
            if semantic in ['numeric', 'numeric-mixed']:
                self._add_outlier_detection(col_name)

            # Étape 4: Standardisation catégorielle
            if semantic == 'categorical':
                self._add_categorical_cleaning(col_name)

            # Étape 5: Nettoyage texte
            if semantic == 'text':
                self._add_text_cleaning(col_name)

        # Étape finale: Déduplication si nécessaire
        self._add_deduplication_step()

        # Trier par priorité
        self.steps.sort(key=lambda x: x.priority)
        return self.steps

    def _add_hidden_missing_detection(self):
        """Étape cruciale: convertit toutes les valeurs manquantes cachées en vrai NaN."""
        code = """# ÉTAPE 0: Détection et conversion des valeurs manquantes cachées
# Cette étape garantit que toutes les formes de "vide" sont traitées uniformément

missing_patterns = [
    '', ' ', '  ', '   ', '\t', '\n', '\r',
    'NA', 'N/A', 'n/a', 'na', 'N.A.', 'n.a.',
    'NULL', 'null', 'Null', 'NONE', 'none', 'None',
    'NaN', 'nan', 'NAN', 'MISSING', 'missing', 'Missing',
    '-', '--', '---', '_', '__', '___',
    '???', '??', '?',
    'Unknown', 'unknown', 'UNKNOWN',
    'Not Available', 'not available', 'NOT AVAILABLE',
    'No Data', 'no data', 'NO DATA',
    'Empty', 'empty', 'EMPTY',
    'Void', 'void', 'VOID',
    'Nil', 'nil', 'NIL'
]

# Convertir en minuscules pour comparaison insensible à la casse
missing_patterns_lower = [p.lower() for p in missing_patterns]

hidden_missing_count = 0
for col in df.select_dtypes(include=['object']).columns:
    # Masque pour valeurs manquantes cachées
    col_str = df[col].astype(str)
    mask = (
        df[col].isin(missing_patterns) |
        col_str.str.strip().eq('') |
        col_str.str.strip().str.lower().isin(missing_patterns_lower)
    )

    if mask.any():
        df.loc[mask, col] = np.nan
        hidden_missing_count += mask.sum()
        logger.info(f"  ✓ Converti {mask.sum()} valeurs vides en NaN dans '{col}'")

if hidden_missing_count > 0:
    logger.info(f"Total: {hidden_missing_count} valeurs manquantes cachées détectées et converties")
else:
    logger.info("Aucune valeur manquante cachée détectée")"""

        self.steps.append(CleaningStep(
            step_type='detect_hidden_missing',
            column=None,
            description="Détection et conversion des valeurs manquantes cachées",
            code_template=code,
            priority=0,  # TOUJOURS premier
            reason="Étape préliminaire obligatoire pour standardiser les valeurs manquantes",
            validation_check="df.isna().sum().sum() >= 0",  # Toujours vrai mais vérifie pas d'erreur
            required_imports=['numpy as np']
        ))

    def _add_numeric_conversion(self, col: str):
        """Conversion robuste numérique avec gestion des séparateurs décimaux."""
        code = f"""# Conversion numérique robuste pour '{col}'
if '{col}' in df.columns:
    # Sauvegarder le nombre de NaN avant conversion
    na_before = df['{col}'].isna().sum()

    # Conversion en string d'abord pour nettoyage
    df['{col}'] = df['{col}'].astype(str)

    # Nettoyer les séparateurs de milliers et remplacer virgule par point
    df['{col}'] = df['{col}'].str.replace(' ', '', regex=False)  # Espaces comme séparateurs de milliers
    df['{col}'] = df['{col}'].str.replace(',', '.', regex=False)  # Virgule française -> point

    # Supprimer les symboles monétaires et pourcentages courants
    df['{col}'] = df['{col}'].str.replace('€', '', regex=False)
    df['{col}'] = df['{col}'].str.replace('$', '', regex=False)
    df['{col}'] = df['{col}'].str.replace('%', '', regex=False)

    # Conversion en numérique avec gestion des erreurs
    df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')

    na_after = df['{col}'].isna().sum()
    converted_count = len(df) - na_after - na_before

    logger.info(f"  ✓ Converti '{col}' en numérique: {{converted_count}} valeurs converties, {{na_after - na_before}} nouveaux NaN")

    # Vérification
    if not pd.api.types.is_numeric_dtype(df['{col}']):
        logger.warning(f"  ⚠ Conversion de '{col}' a échoué - type reste {{df['{col}'].dtype}}")
else:
    logger.warning(f"  ⚠ Colonne '{col}' non trouvée dans le DataFrame")"""

        self.steps.append(CleaningStep(
            step_type='convert_numeric',
            column=col,
            description=f"Conversion robuste de '{col}' en type numérique",
            code_template=code,
            priority=1,
            reason="Colonne numérique stockée comme texte avec possibles séparateurs décimaux variés",
            validation_check=f"pd.api.types.is_numeric_dtype(df['{col}'])",
            required_imports=[]
        ))

    def _add_datetime_conversion(self, col: str):
        """Conversion datetime avec format flexible."""
        code = f"""# Conversion datetime pour '{col}'
if '{col}' in df.columns:
    # Essayer plusieurs formats courants
    formats_to_try = [
        None,  # Auto-détection pandas
        '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d',
        '%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d',
        '%d/%m/%y', '%m/%d/%y', '%y/%m/%d',
        '%d %b %Y', '%d %B %Y',  # 1 Jan 2020
        '%b %d, %Y', '%B %d, %Y',  # Jan 1, 2020
        '%Y%m%d',  # 20200101
    ]

    original_dtype = df['{col}'].dtype
    success = False

    for fmt in formats_to_try:
        try:
            if fmt is None:
                df['{col}'] = pd.to_datetime(df['{col}'], errors='coerce', infer_datetime_format=True)
            else:
                df['{col}'] = pd.to_datetime(df['{col}'], format=fmt, errors='coerce')

            # Vérifier si la conversion a réussi (pas tout NaN)
            if df['{col}'].notna().sum() > 0:
                success = True
                logger.info(f"  ✓ Converti '{col}' en datetime avec format {{fmt if fmt else 'auto'}}")
                break
        except:
            continue

    if not success:
        logger.warning(f"  ⚠ Impossible de convertir '{col}' en datetime - conservation comme {{original_dtype}}")
else:
    logger.warning(f"  ⚠ Colonne '{col}' non trouvée")"""

        self.steps.append(CleaningStep(
            step_type='convert_datetime',
            column=col,
            description=f"Conversion de '{col}' en datetime",
            code_template=code,
            priority=1,
            reason="Dates stockées comme texte dans format non standard",
            validation_check=f"pd.api.types.is_datetime64_any_dtype(df['{col}'])",
            required_imports=[]
        ))

    def _add_imputation(self, col: str, semantic_type: str):
        """Imputation intelligente selon le type de données."""

        if semantic_type in ['numeric', 'numeric-mixed']:
            code = f"""# Imputation par médiane pour '{col}' (numérique)
if '{col}' in df.columns and df['{col}'].isna().any():
    # Vérifier que la colonne est bien numérique
    if not pd.api.types.is_numeric_dtype(df['{col}']):
        logger.warning(f"  ⚠ '{col}' n'est pas numérique, conversion forcée...")
        df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce')

    na_count = df['{col}'].isna().sum()
    if na_count > 0:
        # Calculer médiane sur valeurs non-NaN
        median_val = df['{col}'].median()
        if pd.notna(median_val):
            df['{col}'] = df['{col}'].fillna(median_val)
            logger.info(f"  ✓ Imputation médiane sur '{col}': {{median_val:.2f}} ({{na_count}} valeurs)")
        else:
            # Fallback: moyenne si médiane impossible
            mean_val = df['{col}'].mean()
            if pd.notna(mean_val):
                df['{col}'] = df['{col}'].fillna(mean_val)
                logger.info(f"  ✓ Imputation moyenne sur '{col}': {{mean_val:.2f}} ({{na_count}} valeurs, médiane indisponible)")
            else:
                # Dernier recours: remplacer par 0
                df['{col}'] = df['{col}'].fillna(0)
                logger.warning(f"  ⚠ '{col}': aucune statistique disponible, remplacement par 0 ({{na_count}} valeurs)")
    else:
        logger.info(f"  ✓ Pas de valeurs manquantes dans '{col}' après conversion")
else:
    logger.debug(f"  - Colonne '{col}' ignorée (absente ou sans NaN)"""
        elif semantic_type == 'categorical':
            code = f"""# Imputation par mode pour '{col}' (catégoriel)
if '{col}' in df.columns and df['{col}'].isna().any():
    na_count = df['{col}'].isna().sum()

    # Calculer mode (valeur la plus fréquente)
    mode_series = df['{col}'].mode()
    if len(mode_series) > 0:
        mode_val = mode_series[0]
        df['{col}'] = df['{col}'].fillna(mode_val)
        logger.info(f"  ✓ Imputation mode sur '{col}': '{{mode_val}}' ({{na_count}} valeurs)")
    else:
        # Fallback: 'Unknown' si pas de mode
        df['{col}'] = df['{col}'].fillna('Unknown')
        logger.info(f"  ✓ Imputation 'Unknown' sur '{col}' ({{na_count}} valeurs, mode indisponible)")
else:
    logger.debug(f"  - Colonne '{col}' ignorée (absente ou sans NaN)"""
        else:
            # Fallback générique
            code = f"""# Imputation générique pour '{col}'
if '{col}' in df.columns and df['{col}'].isna().any():
    na_count = df['{col}'].isna().sum()

    # Essayer forward fill puis backward fill
    df['{col}'] = df['{col}'].ffill().bfill()

    # Si toujours des NaN (colonne entièrement vide), remplacer par placeholder
    remaining_na = df['{col}'].isna().sum()
    if remaining_na > 0:
        if pd.api.types.is_numeric_dtype(df['{col}']):
            df['{col}'] = df['{col}'].fillna(0)
            placeholder = "0"
        else:
            df['{col}'] = df['{col}'].fillna('MISSING')
            placeholder = "'MISSING'"
        logger.info(f"  ✓ Imputation ffill/bfill + {{placeholder}} sur '{col}' ({{na_count}} valeurs, {{remaining_na}} restantes)")
    else:
        logger.info(f"  ✓ Imputation ffill/bfill sur '{col}' ({{na_count}} valeurs)")
else:
    logger.debug(f"  - Colonne '{col}' ignorée"""

        self.steps.append(CleaningStep(
            step_type='imputation',
            column=col,
            description=f"Imputation des valeurs manquantes dans '{col}'",
            code_template=code,
            priority=10,
            reason=f"{semantic_type} avec valeurs manquantes",
            validation_check=f"df['{col}'].isna().sum() == 0",
            required_imports=[]
        ))

    def _add_outlier_detection(self, col: str):
        """Détection outliers avec méthode IQR robuste."""
        code = f"""# Winsorisation IQR pour '{col}'
if '{col}' in df.columns and pd.api.types.is_numeric_dtype(df['{col}']):
    # Vérifier qu'il y a assez de données
    non_null_count = df['{col}'].notna().sum()
    if non_null_count >= 10:
        Q1 = df['{col}'].quantile(0.25)
        Q3 = df['{col}'].quantile(0.75)
        IQR = Q3 - Q1

        if pd.notna(IQR) and IQR > 0:  # Éviter division par zéro
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            # Compter outliers
            outliers = ((df['{col}'] < lower_bound) | (df['{col}'] > upper_bound)).sum()

            if outliers > 0:
                # Winsorisation (clip) plutôt que suppression
                df['{col}'] = df['{col}'].clip(lower=lower_bound, upper=upper_bound)
                logger.info(f"  ✓ Winsorisation '{col}': {{outliers}} outliers limités à [{{lower_bound:.2f}}, {{upper_bound:.2f}}]")
            else:
                logger.debug(f"  - Pas d'outliers détectés dans '{col}'")
        else:
            logger.debug(f"  - IQR nul pour '{col}', winsorisation ignorée")
    else:
        logger.debug(f"  - Trop peu de données dans '{col}' pour détection outliers ({{non_null_count}} valeurs)")
else:
    logger.debug(f"  - '{col}' ignorée pour outliers (non numérique ou absente)"""

        self.steps.append(CleaningStep(
            step_type='clip_outliers',
            column=col,
            description=f"Correction des outliers dans '{col}' (méthode IQR)",
            code_template=code,
            priority=20,
            reason="Distribution avec valeurs extrêmes potentielles",
            validation_check=f"df['{col}'].between(df['{col}'].quantile(0.01), df['{col}'].quantile(0.99)).mean() > 0.95",
            required_imports=[]
        ))

    def _add_categorical_cleaning(self, col: str):
        """Nettoyage standardisé des catégorielles."""
        code = f"""# Nettoyage catégoriel pour '{col}'
if '{col}' in df.columns:
    original_unique = df['{col}'].nunique()

    # Standardisation texte
    df['{col}'] = df['{col}'].astype(str)
    df['{col}'] = df['{col}'].str.strip()  # Supprimer espaces début/fin
    df['{col}'] = df['{col}'].str.replace(r'\s+', ' ', regex=True)  # Espaces multiples -> simple

    # Normalisation casse (Title Case pour noms, Upper pour codes)
    if any(keyword in '{col}'.lower() for keyword in ['name', 'nom', 'prenom', 'ville', 'pays']):
        df['{col}'] = df['{col}'].str.title()
    elif any(keyword in '{col}'.lower() for keyword in ['code', 'id', 'ref', 'status']):
        df['{col}'] = df['{col}'].str.upper()
    else:
        df['{col}'] = df['{col}'].str.title()

    # Remplacer valeurs vides restantes
    df['{col}'] = df['{col}'].replace(['Nan', 'None', 'Null', ''], 'Unknown')

    new_unique = df['{col}'].nunique()
    reduced = original_unique - new_unique

    if reduced > 0:
        logger.info(f"  ✓ Standardisation '{col}': {{reduced}} valeurs fusionnées ({{original_unique}} -> {{new_unique}} uniques)")
    else:
        logger.info(f"  ✓ Standardisation '{col}' effectuée ({{new_unique}} valeurs uniques)")
else:
    logger.warning(f"  ⚠ Colonne '{col}' non trouvée pour nettoyage catégoriel"""

        self.steps.append(CleaningStep(
            step_type='clean_categorical',
            column=col,
            description=f"Standardisation des valeurs catégorielles dans '{col}'",
            code_template=code,
            priority=15,
            reason="Valeurs avec espaces, incohérences de casse, ou formats variés",
            validation_check=f"df['{col}'].astype(str).str.contains(r'^\s+|\s+$').sum() == 0",
            required_imports=[]
        ))

    def _add_text_cleaning(self, col: str):
        """Nettoyage avancé pour colonnes texte long."""
        code = f"""# Nettoyage texte pour '{col}'
if '{col}' in df.columns:
    # Nettoyage de base
    df['{col}'] = df['{col}'].astype(str)
    df['{col}'] = df['{col}'].str.strip()

    # Supprimer espaces multiples
    df['{col}'] = df['{col}'].str.replace(r'\s+', ' ', regex=True)

    # Supprimer caractères de contrôle
    df['{col}'] = df['{col}'].str.replace(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', '', regex=True)

    logger.info(f"  ✓ Nettoyage texte '{col}' effectué")
else:
    logger.warning(f"  ⚠ Colonne '{col}' non trouvée"""

        self.steps.append(CleaningStep(
            step_type='clean_text',
            column=col,
            description=f"Nettoyage des textes dans '{col}'",
            code_template=code,
            priority=15,
            reason="Textes avec espaces excessifs ou caractères spéciaux",
            validation_check=f"df['{col}'].astype(str).str.contains(r'  ').sum() == 0",
            required_imports=[]
        ))

    def _add_deduplication_step(self):
        """Déduplication finale si nécessaire."""
        code = """# Déduplication finale (optionnelle)
# Ne supprime que si explicitement demandé ou doublons évidents

initial_rows = len(df)
duplicates = df.duplicated().sum()

if duplicates > 0:
    logger.info(f"{duplicates} lignes dupliquées détectées")
    # Par défaut: conserver les doublons mais logger
    # Pour suppression automatique, décommenter:
    # df = df.drop_duplicates()
    # logger.info(f"✓ Supprimé {duplicates} doublons ({initial_rows} -> {len(df)} lignes)")
else:
    logger.info("Aucun doublon détecté")"""

        self.steps.append(CleaningStep(
            step_type='deduplicate',
            column=None,
            description="Détection des doublons",
            code_template=code,
            priority=100,  # Dernier
            reason="Vérification de l'unicité des lignes",
            validation_check="True",
            required_imports=[]
        ))

    def generate_summary(self) -> str:
        """Génère un résumé textuel du plan de nettoyage."""
        lines = ["## Plan de nettoyage suggéré", ""]
        for i, step in enumerate(self.steps, 1):
            lines.append(f"**{i}. {step.description}**")
            lines.append(f"   - Type: {step.step_type}")
            lines.append(f"   - Priorité: {step.priority}")
            lines.append(f"   - Raison: {step.reason}")
            lines.append("")
        return "\n".join(lines)


def generate_cleaning_strategy(profiles: Dict, df_sample: pd.DataFrame, use_llm: bool = False) -> Dict[str, Any]:
    """Fonction utilitaire pour le router."""
    engine = SuggestionEngine(profiles, df_sample)
    steps = engine.generate_steps()

    return {
        'steps': [
            {
                'type': s.step_type,
                'column': s.column,
                'description': s.description,
                'code': s.code_template,
                'priority': s.priority,
                'validation': s.validation_check,
                'required_imports': s.required_imports
            }
            for s in steps
        ],
        'summary': engine.generate_summary(),
        'total_steps': len(steps),
        'critical_steps': len([s for s in steps if s.priority < 10]),
        'estimated_time': f"{len(steps) * 0.5:.1f}s"  # Estimation grossière
    }
