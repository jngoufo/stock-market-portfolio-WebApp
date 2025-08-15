import pandas as pd
import configparser
from sqlalchemy import create_engine, text
import os
import logging
import re

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
os.chdir(os.path.dirname(os.path.abspath(__file__)))

try:
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['database']
except Exception as e:
    logging.error(f"Erreur de lecture de config.ini: {e}")
    exit()

logging.info("--- Début de la mise à jour des quantités ---")

def clean_value(value):
    if isinstance(value, str):
        return re.sub(r'[^0-9.-]', '', value)
    return value

try:
    # 1. Lire le nouveau CSV pour obtenir les quantités
    logging.info("Lecture du fichier source pour les quantités...")
    df_source = pd.read_csv('./source/tipranks_raw.csv')
    df_source.columns = [col.strip().lower().replace(' ', '_') for col in df_source.columns]
    df_source['quantite'] = pd.to_numeric(df_source['no._of_shares'].apply(clean_value), errors='coerce')
    quantites_actuelles = df_source.set_index('ticker')['quantite'].to_dict()

    # 2. Connexion à la base de données
    connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        trans = conn.begin()
        
        # 3. Récupérer tous les titres de la base de données
        tous_les_titres = conn.execute(text("SELECT id, ticker FROM titres")).fetchall()
        logging.info(f"{len(tous_les_titres)} titres trouvés dans la base de données.")

        for titre_id, ticker in tous_les_titres:
            try:
                # 4. Trouver la nouvelle quantité dans le CSV
                nouvelle_quantite = quantites_actuelles.get(ticker)

                if nouvelle_quantite is not None and not pd.isna(nouvelle_quantite):
                    # 5. Trouver le dernier relevé historique pour ce titre
                    dernier_releve_stmt = text("SELECT id FROM historique WHERE titre_id = :id ORDER BY date_releve DESC LIMIT 1")
                    result = conn.execute(dernier_releve_stmt, {'id': titre_id}).fetchone()

                    if result:
                        dernier_releve_id = result[0]
                        # 6. Mettre à jour uniquement la quantité de ce dernier relevé
                        update_stmt = text("UPDATE historique SET quantite = :qte WHERE id = :id")
                        conn.execute(update_stmt, {'qte': int(nouvelle_quantite), 'id': dernier_releve_id})
                        logging.info(f"Quantité pour {ticker} mise à jour à {int(nouvelle_quantite)}.")
                    else:
                        logging.warning(f"Aucun historique trouvé pour {ticker}, aucune mise à jour effectuée.")
                else:
                    logging.warning(f"Aucune nouvelle quantité trouvée pour {ticker} dans le CSV, titre ignoré.")

            except Exception as row_error:
                logging.error(f"Erreur lors du traitement du ticker {ticker}: {row_error}")
        
        trans.commit()
        logging.info("Transaction terminée.")

except Exception as e:
    logging.error(f"Une erreur majeure est survenue : {e}", exc_info=True)

logging.info("--- Mise à jour des quantités terminée ---")