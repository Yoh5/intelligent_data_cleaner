import pandas as pd
import numpy as np
import random

# Génération du dataset de test complet
np.random.seed(42)
n = 1000

data = {
    # 1. ID avec doublons
    'customer_id': list(range(1, 995)) + [1, 2, 3, 4, 5],  # Doublons aux IDs 1-5
    
    # 2. Numérique avec valeurs manquantes cachées (types mixtes)
    'total_charges': [random.choice([' ', 'NA', 'null', '123.45', '678.90', '', 'N/A']) 
                      if random.random() < 0.15 else f"{random.uniform(0, 1000):.2f}"
                      for _ in range(n)],
    
    # 3. Numérique avec outliers
    'age': [random.randint(18, 80) for _ in range(980)] + [150, 999, -5, 200, 300],  # Outliers en fin
    
    # 4. Catégoriel avec incohérences de format
    'city': random.choices(['Paris', 'paris', 'PARIS', 'Lyon ', ' lyon', 'LYON', 
                           'Marseille', 'N/A', 'unknown', None], k=n),
    
    # 5. Email avec PII (données sensibles)
    'email': [f"user{i}@gmail.com" if random.random() > 0.2 else f"client{i}@entreprise.fr" 
              for i in range(n)],
    
    # 6. Date avec formats inconsistants
    'signup_date': random.choices(['2023-01-15', '15/01/2023', 'Jan 15, 2023', 
                                   '2023-01-15 14:30:00', '', 'N/A', 'None'], k=n),
    
    # 7. Booléen avec valeurs multiples représentations
    'is_active': random.choices(['Yes', 'yes', 'YES', '1', 'No', 'no', 'NO', '0', 
                                 'true', 'false', True, False, 1, 0, None], k=n),
    
    # 8. Colonne quasi-vide (très peu de données)
    'optional_field': [None] * 995 + ['data'] * 5,
    
    # 9. Texte long avec caractères spéciaux
    'comments': random.choices(['Bon client', '   Mauvais payeur   ', 'N/A', 
                                'Réclamation urgente!!!', None, 'Client\nmultiligne\twith tabs'], k=n),
    
    # 10. Valeur constante (colonne inutile)
    'constant_col': ['SAME_VALUE'] * n
}

df_test = pd.DataFrame(data)

# Sauvegarde avec des problèmes d'encoding volontaires
df_test.to_csv('test_dirty_dataset.csv', index=False, encoding='utf-8')
print(f"Dataset créé: {df_test.shape}")
print("\nAperçu des problèmes:")
print(df_test.head(10))