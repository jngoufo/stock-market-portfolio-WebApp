import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
os.chdir(os.path.dirname(os.path.abspath(__file__)))

try:
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['database']
except Exception as e:
    logging.error(f"Erreur de lecture de config.ini: {e}")
    exit()

logging.info("--- Début de l'importation des données ---")

def clean_currency(value):
    if isinstance(value, str): return re.sub(r'[^0-9.-]', '', value)
    return value
def detect_currency(value):
    if isinstance(value, str) and 'c$' in value.lower(): return 'CAD'
    return 'USD'

try:
    logging.info("Lecture du fichier source...")
    df = pd.read_csv('./source/tipranks_raw.csv')
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    df.dropna(subset=['ticker'], inplace=True)

    logging.info("Connexion à la base de données...")
    connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(connection_string)

    utc_now = datetime.now(ZoneInfo("UTC"))
    montreal_now = utc_now.astimezone(ZoneInfo("America/Montreal"))
    date_du_releve = montreal_now.strftime('%Y-%m-%d')
    logging.info(f"Date du relevé : {date_du_releve}")
    
    with engine.connect() as conn:
        trans = conn.begin()
        logging.info("Connexion réussie. Début de la transaction.")

        logging.info("Synchronisation des titres...")
        tickers_in_csv = set(df['ticker'].str.lower())
        
        result = conn.execute(text("SELECT ticker FROM titres"))
        tickers_in_db = {row[0].lower() for row in result}

        tickers_to_delete = tickers_in_db - tickers_in_csv
        
        if tickers_to_delete:
            logging.info(f"Titres à supprimer : {', '.join(tickers_to_delete)}")
            for ticker_to_del in tickers_to_delete:
                titre_id_result = conn.execute(text("SELECT id FROM titres WHERE ticker = :ticker"), {'ticker': ticker_to_del}).fetchone()
                if titre_id_result:
                    titre_id = titre_id_result[0]
                    conn.execute(text("DELETE FROM historique WHERE titre_id = :id"), {'id': titre_id})
                    conn.execute(text("DELETE FROM titres WHERE id = :id"), {'id': titre_id})
                    logging.info(f"Titre '{ticker_to_del}' et son historique supprimés.")
        else:
            logging.info("Aucun titre à supprimer.")

        for index, row in df.iterrows():
            try:
                ticker = row.get('ticker')
                if ticker and ticker.lower() == 'cash':
                    logging.info("Ligne 'Cash' ignorée.")
                    continue

                nom_entreprise = row.get('name')
                quantite = pd.to_numeric(clean_currency(row.get('no._of_shares')), errors='coerce')
                valeur = pd.to_numeric(clean_currency(row.get('price')), errors='coerce')
                devise = detect_currency(row.get('holding_value', ''))

                if pd.isna(ticker) or pd.isna(nom_entreprise) or pd.isna(quantite) or pd.isna(valeur):
                    logging.warning(f"Ligne ignorée pour le ticker {ticker} car des données sont manquantes.")
                    continue

                quantite = int(quantite)
                valeur = float(valeur)

                result = conn.execute(text("SELECT id FROM titres WHERE ticker = :ticker"), {'ticker': ticker}).fetchone()
                
                if result:
                    titre_id = int(result[0])
                else:
                    insert_titre_stmt = text("INSERT INTO titres (ticker, nom_entreprise) VALUES (:ticker, :nom)")
                    cursor = conn.execute(insert_titre_stmt, {'ticker': ticker, 'nom': nom_entreprise})
                    titre_id = int(cursor.lastrowid)
                    logging.info(f"Nouveau titre créé : {nom_entreprise} (ID: {titre_id})")
                
                check_histo_stmt = text("SELECT id FROM historique WHERE titre_id = :id AND date_releve = :date")
                existing_histo = conn.execute(check_histo_stmt, {'id': titre_id, 'date': date_du_releve}).fetchone()
                
                if existing_histo:
                    logging.info(f"Un relevé existe déjà pour {ticker} à la date {date_du_releve}. Ligne ignorée.")
                    continue

                insert_histo_stmt = text("""
                    INSERT INTO historique (titre_id, date_releve, valeur, quantite, devise) 
                    VALUES (:id, :date, :val, :qte, :devise)
                """)
                conn.execute(insert_histo_stmt, {'id': titre_id, 'date': date_du_releve, 'val': valeur, 'qte': quantite, 'devise': devise})

            except Exception as row_error:
                logging.error(f"Erreur lors du traitement de la ligne pour le ticker {row.get('ticker')}: {row_error}")
        
        trans.commit()
        logging.info(f"Transaction terminée.")
except Exception as e:
    logging.error(f"Une erreur majeure est survenue : {e}", exc_info=True)
logging.info("--- Importation terminée ---")