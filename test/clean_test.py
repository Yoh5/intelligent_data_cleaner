#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de nettoyage automatique - Généré le 2026-03-22 19:27:23
Source: test.csv
Ce script résout les problèmes suivants:
#   - missing_values sur customerid
#   - mixed_types sur invoiceno
#   - mixed_types sur stockcode
#   - missing_values sur description
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys
import argparse
from typing import Optional, Union
import warnings
warnings.filterwarnings('ignore')

INPUT_FILE: Optional[str] = None
OUTPUT_FILE: Optional[str] = None
VERBOSE: bool = True

def setup_logging(verbose: bool = True) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)

logger = setup_logging(VERBOSE)

def detect_encoding(file_path):
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(1024)
            return encoding
        except:
            continue
    return 'utf-8'

def detect_delimiter(file_path, encoding):
    delimiters = [',', ';', '\t', '|']
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            first_line = f.readline()
            counts = {d: first_line.count(d) for d in delimiters}
            best = max(counts, key=counts.get)
            return best if counts[best] > 0 else ','
    except:
        return ','

def load_data(file_path):
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f'Fichier non trouvé: {file_path}')
    encoding = detect_encoding(file_path)
    delimiter = detect_delimiter(file_path, encoding)
    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, skipinitialspace=True)
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(' ', '_')
    return df

def clean_data(df):
    logger.info('\n' + '='*60)
    logger.info('DÉBUT DU NETTOYAGE')
    logger.info('='*60)
    df_clean = df.copy()
    initial_rows = len(df_clean)
    # ÉTAPES DE NETTOYAGE:

    # ============================================================
    # ÉTAPE 1: missing_values sur 'customerid'
    # ============================================================
    try:
        if 'customerid' in df.columns:
            df['customerid'] = pd.to_numeric(df['customerid'], errors='coerce')
            df['customerid'] = df['customerid'].fillna(df['customerid'].median())
    except Exception as step_error:
        logger.warning("Étape 1 ignorée: " + str(step_error))

    # ============================================================
    # ÉTAPE 2: mixed_types sur 'invoiceno'
    # ============================================================
    try:
        if 'invoiceno' in df.columns:
            df['invoiceno'] = df['invoiceno'].astype(str).str.replace(',', '.')
            df['invoiceno'] = pd.to_numeric(df['invoiceno'], errors='coerce')
    except Exception as step_error:
        logger.warning("Étape 2 ignorée: " + str(step_error))

    # ============================================================
    # ÉTAPE 3: mixed_types sur 'stockcode'
    # ============================================================
    try:
        if 'stockcode' in df.columns:
            df['stockcode'] = df['stockcode'].astype(str).str.replace(',', '.')
            df['stockcode'] = pd.to_numeric(df['stockcode'], errors='coerce')
    except Exception as step_error:
        logger.warning("Étape 3 ignorée: " + str(step_error))

    # ============================================================
    # ÉTAPE 4: missing_values sur 'description'
    # ============================================================
    try:
        if 'description' in df.columns:
            df['description'] = pd.to_numeric(df['description'], errors='coerce')
            df['description'] = df['description'].fillna(df['description'].median())
    except Exception as step_error:
        logger.warning("Étape 4 ignorée: " + str(step_error))

    logger.info('\n' + '='*60)
    logger.info('RESULTAT')
    logger.info(f'Dimensions: {initial_rows} -> {len(df_clean)} lignes')
    return df_clean

def save_data(df, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f'Sauvegardé: {output_path}')

def main():
    parser = argparse.ArgumentParser(description='Nettoyage CSV')
    parser.add_argument('--input', '-i', help='Fichier entrée')
    parser.add_argument('--output', '-o', help='Fichier sortie')
    args = parser.parse_args()
    input_file = args.input or INPUT_FILE
    output_file = args.output or OUTPUT_FILE
    if not input_file:
        parser.error('Spécifiez un fichier entrée')
    if not output_file:
        output_file = f'cleaned_{Path(input_file).stem}.csv'
    df_raw = load_data(input_file)
    df_clean = clean_data(df_raw)
    save_data(df_clean, output_file)
    logger.info('NETTOYAGE TERMINÉ')

if __name__ == "__main__":
    main()