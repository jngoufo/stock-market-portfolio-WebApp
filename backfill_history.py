import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os
import logging
import re
import yfinance as yf

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
os.chdir(os.path.dirname(os.path.abspath(__file__)))
config = configparser.ConfigParser()
config.read('config.ini')
db_config = config['database']

# --- PARAMÈTRES (MODIFIÉS) ---
START_DATE = '2025-08-01'
END_DATE = '2025-08-15' # On met le 15 pour que yfinance inclue bien le 14

logging.info(f"--- Début du Remplacement de l'Historique ({START_DATE} -> aujourd'hui) ---")

def clean_currency_value(value):
    if isinstance(value, str):
        return re.sub(r'[^0-9.-]', '', value)
    return value

def detect_currency(value):
    if isinstance(value, str) and 'c$' in value.lower():
        return 'CAD'
    return 'USD'

try:
    # 1. Lire le CSV pour obtenir la liste des titres et leur quantité actuelle
    logging.info("Lecture du fichier source pour les quantités...")
    df_source = pd.read_csv('./source/tipranks_raw.csv')
    df_source.columns = [col.strip().lower().replace(' ', '_') for col in df_source.columns]
    df_source['quantite'] = pd.to_numeric(df_source['no._of_shares'].apply(clean_currency_value), errors='coerce')
    quantites_actuelles = df_source.set_index('ticker')['quantite'].to_dict()
    devises = df_source.set_index('ticker')['holding_value'].apply(detect_currency).to_dict()

    # 2. Connexion à la base de données
    connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        trans = conn.begin()
        
        # 3. Supprimer l'historique existant pour la période concernée
        logging.info(f"Suppression de l'historique à partir du {START_DATE}...")
        delete_stmt = text("DELETE FROM historique WHERE date_releve >= :start")
        conn.execute(delete_stmt, {'start': START_DATE})

        # 4. Récupérer tous les titres de la base de données
        tous_les_titres = conn.execute(text("SELECT id, ticker FROM titres")).fetchall()
        logging.info(f"{len(tous_les_titres)} titres à traiter.")

        for titre_id, ticker_original in tous_les_titres:
            try:
                # 5. Récupérer la quantité et la devise pour ce titre
                quantite = quantites_actuelles.get(ticker_original, 0)
                devise = devises.get(ticker_original, 'USD')

                if quantite == 0:
                    logging.warning(f"Aucune quantité trouvée pour {ticker_original}, titre ignoré.")
                    continue

                # 6. Traduire le ticker et récupérer l'historique yfinance
                ticker_yf = ticker_original.strip().upper()
                if ticker_original.upper().startswith('TSE:'):
                    base_ticker = ticker_original.split(':')[1].replace('.', '-')
                    ticker_yf = base_ticker.upper() + '.TO'
                
                stock = yf.Ticker(ticker_yf)
                hist = stock.history(start=START_DATE, end=END_DATE)
                
                if hist.empty:
                    logging.warning(f"Aucun historique yfinance trouvé pour {ticker_original} ({ticker_yf})")
                    continue
                
                # 7. Préparer les données pour une insertion en masse
                donnees_a_inserer = []
                for date_releve, row in hist.iterrows():
                    donnees_a_inserer.append({
                        'id': titre_id,
                        'date': date_releve.strftime('%Y-%m-%d'),
                        'val': row['Close'],
                        'qte': quantite,
                        'devise': devise
                    })

                # 8. Insérer les nouvelles données
                if donnees_a_inserer:
                    insert_stmt = text("""
                        INSERT INTO historique (titre_id, date_releve, valeur, quantite, devise) 
                        VALUES (:id, :date, :val, :qte, :devise)
                    """)
                    conn.execute(insert_stmt, donnees_a_inserer)
                    logging.info(f"{len(donnees_a_inserer)} points de données historiques insérés pour {ticker_original}.")

            except Exception as row_error:
                logging.error(f"Erreur lors du traitement de {ticker_original}: {row_error}")
        
        trans.commit()
        logging.info("Transaction terminée.")

except Exception as e:
    logging.error(f"Une erreur majeure est survenue : {e}", exc_info=True)

logging.info("--- Remplacement de l'historique terminé ---")