#!/usr/bin/env python3
import pandas as pd
import numpy as np

def clean_data(input_file: str, output_file: str):
    df = pd.read_csv(input_file)
    
    # Nettoyage basique
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
    
    df.to_csv(output_file, index=False)
    print(f"Nettoyage basique: {len(df)} lignes")

if __name__ == "__main__":
    clean_data("test.csv", "cleaned_test.csv")
