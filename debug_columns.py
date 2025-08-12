import pandas as pd
import os

# S'assure qu'on est dans le bon dossier
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Lit le fichier CSV
df = pd.read_csv('./source/tipranks_raw.csv')

# Nettoie les noms de colonnes de la même manière que le script d'importation
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

# Affiche la liste des noms de colonnes trouvés
print("Voici les noms des colonnes trouvées dans votre fichier CSV :")
print(df.columns.tolist())