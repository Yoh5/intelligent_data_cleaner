#!/usr/bin/env python3
"""
Script de secours - Nettoyage basique
"""
import pandas as pd
import numpy as np

def clean_data(input_file: str, output_file: str):
    df = pd.read_csv(input_file)
    
    # Nettoyage basique
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
    
    # Sauvegarde
    df.to_csv(output_file, index=False)
    print(f"Nettoyage basique effectué: {len(df)} lignes")

if __name__ == "__main__":
    clean_data("WA_Fn-UseC_-Telco-Customer-Churn.csv", "cleaned_WA_Fn-UseC_-Telco-Customer-Churn.csv")
