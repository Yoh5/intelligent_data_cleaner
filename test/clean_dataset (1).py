#!/usr/bin/env python3
"""
Script de nettoyage de données généré automatiquement
Source: WA_Fn-UseC_-Telco-Customer-Churn.csv
Generated: 20260321_211731
Steps: 2
Ordre: Conversion de type → Imputation → Outliers
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

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

def detect_hidden_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte et nettoie les valeurs manquantes cachées avant traitement.
    """
    missing_patterns = ['', ' ', '  ', '   ', 'NA', 'N/A', 'null', 'NULL', 'None', 'NaN']
    
    for col in df.select_dtypes(include=['object']):
        mask = df[col].isin(missing_patterns) | df[col].astype(str).str.strip().eq('')
        if mask.any():
            df.loc[mask, col] = np.nan
            logger.info(f"Converti {mask.sum()} valeurs vides en NaN dans '{col}'")
    
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de nettoyage principal.
    ORDRE CRITIQUE: Conversion type → Imputation → Outliers
    """
    logger.info(f"Données initiales: {len(df)} lignes, {len(df.columns)} colonnes")
    
    # Backup
    df_clean = df.copy()
    
    # Pré-traitement des valeurs manquantes cachées
    df_clean = detect_hidden_missing(df_clean)
    
    # ETAPES DE NETTOYAGE (triées: conversion avant imputation)
    # Step 1: mixed_types - high
    # Column: TotalCharges
    try:
        # Conversion type pour TotalCharges (AVANT imputation)
        df_clean['TotalCharges'] = df_clean['TotalCharges'].replace(r'^\s*$', np.nan, regex=True)
        df_clean['TotalCharges'] = pd.to_numeric(df_clean['TotalCharges'], errors='coerce')
        logger.info(f"Conversion TotalCharges en numérique")
    except Exception as e:
        logger.warning(f"Step 1 failed: {e}")

    # Step 2: missing_values - medium
    # Column: TotalCharges
    try:
        # Imputation pour TotalCharges (APRÈS conversion type si nécessaire)
        if df_clean['TotalCharges'].isna().any():
            if pd.api.types.is_numeric_dtype(df_clean['TotalCharges']):
                median_val = df_clean['TotalCharges'].median()
                if pd.notna(median_val):
                    df_clean['TotalCharges'] = df_clean['TotalCharges'].fillna(median_val)
                    logger.info(f"Imputation médiane TotalCharges: {median_val:.2f}")
                else:
                    df_clean['TotalCharges'] = df_clean['TotalCharges'].fillna(0)
            else:
                mode_val = df_clean['TotalCharges'].mode()
                if len(mode_val) > 0:
                    df_clean['TotalCharges'] = df_clean['TotalCharges'].fillna(mode_val[0])
                    logger.info(f"Imputation mode TotalCharges: {mode_val[0]}")
    except Exception as e:
        logger.warning(f"Step 2 failed: {e}")
    
    # Vérification finale
    final_missing = df_clean.isna().sum().sum()
    logger.info(f"Nettoyage terminé: {final_missing} valeurs manquantes restantes")
    
    if final_missing > 0:
        cols_with_missing = df_clean.columns[df_clean.isna().any()].tolist()
        logger.warning(f"Colonnes avec valeurs manquantes: {cols_with_missing}")
    
    return df_clean

def save_data(df: pd.DataFrame, output_path: str):
    """Sauvegarde avec vérification."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Sauvegarde dans {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8')

def main():
    INPUT_FILE = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
    OUTPUT_FILE = "cleaned_" + Path(INPUT_FILE).stem + ".csv"
    
    try:
        df_raw = load_data(INPUT_FILE)
        df_clean = clean_data(df_raw)
        save_data(df_clean, OUTPUT_FILE)
        
        print(f"\n✅ Nettoyage terminé")
        print(f"   Fichier: {OUTPUT_FILE}")
        print(f"   Lignes: {len(df_clean)}")
        
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        raise

if __name__ == "__main__":
    main()
