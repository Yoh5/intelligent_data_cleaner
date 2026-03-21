#!/usr/bin/env python3
"""
Script de nettoyage de données généré automatiquement
Source: WA_Fn-UseC_-Telco-Customer-Churn.csv
Generated: 20260321_210818
Steps: 2
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_data(filepath: str) -> pd.DataFrame:
    """Charge le dataset avec détection automatique."""
    logger.info(f"Chargement de {filepath}")
    
    path = Path(filepath)
    suffix = path.suffix.lower()
    
    if suffix == '.csv':
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
            try:
                df = pd.read_csv(filepath, encoding=encoding)
                logger.info(f"Encoding détecté: {encoding}")
                return df
            except UnicodeDecodeError:
                continue
        raise ValueError("Impossible de décoder le fichier CSV")
    
    elif suffix in ['.xlsx', '.xls']:
        return pd.read_excel(filepath)
    
    else:
        raise ValueError(f"Format non supporté: {suffix}")

def detect_and_clean_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte et nettoie les valeurs manquantes cachées (strings vides, 'NA', etc.)
    avant le traitement principal.
    """
    missing_patterns = ['', ' ', '  ', '   ', 'NA', 'N/A', 'null', 'NULL', 'None', 'NaN']
    
    for col in df.select_dtypes(include=['object']).columns:
        mask = df[col].isin(missing_patterns) | df[col].astype(str).str.strip().eq('')
        if mask.any():
            df.loc[mask, col] = np.nan
            logger.info(f"Converti {mask.sum()} valeurs vides en NaN dans '{col}'")
    
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de nettoyage principal.
    Dataset initial: 2 étapes définies
    """
    logger.info(f"Données initiales: {len(df)} lignes, {len(df.columns)} colonnes")
    
    # Backup
    df_clean = df.copy()
    
    # Pré-traitement des valeurs manquantes cachées
    df_clean = detect_and_clean_missing(df_clean)
    
    # Application des étapes
    # Step 1: missing_values - medium
    # Column: TotalCharges
    try:
        # Imputation pour TotalCharges
        if df['TotalCharges'].dtype in ['object']:
            df['TotalCharges'] = df['TotalCharges'].fillna(df['TotalCharges'].mode()[0] if not df['TotalCharges'].mode().empty else 'Inconnu')
        else:
            df['TotalCharges'] = df['TotalCharges'].fillna(df['TotalCharges'].median())
    except Exception as e:
        logger.warning(f"Step 1 failed: {e}")

    # Step 2: mixed_types - high
    # Column: TotalCharges
    try:
        # Conversion type pour TotalCharges
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    except Exception as e:
        logger.warning(f"Step 2 failed: {e}")
    
    # Log des résultats
    final_missing = df_clean.isna().sum().sum()
    logger.info(f"Nettoyage terminé: {final_missing} valeurs manquantes restantes")
    logger.info(f"Données finales: {len(df_clean)} lignes, {len(df_clean.columns)} colonnes")
    
    return df_clean

def save_data(df: pd.DataFrame, output_path: str):
    """Sauvegarde avec vérification."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Sauvegarde dans {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    # Vérification
    saved_df = pd.read_csv(output_path)
    logger.info(f"Vérification: {len(saved_df)} lignes sauvegardées")

def generate_report(df_before: pd.DataFrame, df_after: pd.DataFrame) -> dict:
    """Rapport comparatif détaillé."""
    report = {
        "lignes_avant": int(len(df_before)),
        "lignes_apres": int(len(df_after)),
        "lignes_supprimees": int(len(df_before) - len(df_after)),
        "colonnes": int(len(df_after.columns)),
        "valeurs_manquantes_avant": int(df_before.isna().sum().sum()),
        "valeurs_manquantes_apres": int(df_after.isna().sum().sum()),
        "modifications_appliquees": 2
    }
    
    logger.info("=" * 50)
    logger.info("RAPPORT DE NETTOYAGE")
    for key, value in report.items():
        logger.info(f"  {key}: {value}")
    logger.info("=" * 50)
    
    return report

def validate_output(df: pd.DataFrame) -> bool:
    """Validation finale des données."""
    issues = []
    
    if df.empty:
        issues.append("Dataset vide après nettoyage")
    
    if df.isna().all().any():
        cols = df.columns[df.isna().all()].tolist()
        issues.append(f"Colonnes entièrement vides: {cols}")
    
    if issues:
        for issue in issues:
            logger.error(f"Validation failed: {issue}")
        return False
    
    return True

def main():
    # Configuration - MODIFIEZ CES VALEURS
    INPUT_FILE = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
    OUTPUT_FILE = "cleaned_" + Path(INPUT_FILE).stem + ".csv"
    
    try:
        # 1. Chargement
        df_raw = load_data(INPUT_FILE)
        
        # 2. Nettoyage
        df_clean = clean_data(df_raw)
        
        # 3. Validation
        if not validate_output(df_clean):
            logger.error("Validation échouée - arrêt")
            return
        
        # 4. Rapport
        report = generate_report(df_raw, df_clean)
        
        # 5. Sauvegarde
        save_data(df_clean, OUTPUT_FILE)
        
        print(f"\n✅ Nettoyage terminé avec succès")
        print(f"   Fichier sortie: {OUTPUT_FILE}")
        print(f"   Lignes traitées: {report['lignes_apres']}")
        
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        raise

if __name__ == "__main__":
    main()
