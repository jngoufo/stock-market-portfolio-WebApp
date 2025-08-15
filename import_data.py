import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import logging
import re
import yfinance as yf

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

logging.info("--- Début de l'importation des données ---")

def clean_currency(value):
    if isinstance(value, str):
        return re.sub(r'[^0-9.-]', '', value)
    return value
def detect_currency(value):
    if isinstance(value, str) and 'c$' in value.lower():
        return 'CAD'
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
        
        # --- CORRECTIF : Comparaison insensible à la casse ---
        logging.info("Synchronisation des titres...")
        tickers_in_csv = set(df['ticker'].str.lower())
        
        result = conn.execute(text("SELECT id, ticker FROM titres"))
        titres_in_db = {row[1].lower(): row[0] for row in result}
        tickers_in_db_set = set(titres_in_db.keys())

        tickers_to_delete = tickers_in_db_set - tickers_in_csv
        
        if tickers_to_delete:
            logging.info(f"Titres à supprimer : {', '.join(tickers_to_delete)}")
            ids_to_delete = [titres_in_db[ticker] for ticker in tickers_to_delete]
            
            if ids_to_delete:
                ids_tuple = tuple(ids_to_delete)
                conn.execute(text("DELETE FROM historique WHERE titre_id IN :ids"), {'ids': ids_tuple})
                conn.execute(text("DELETE FROM titres WHERE id IN :ids"), {'ids': ids_tuple})
                logging.info(f"{len(ids_to_delete)} titres ont été supprimés.")
        else:
            logging.info("Aucun titre à supprimer.")

        for index, row in df.iterrows():
            try:
                ticker = row.get('ticker')
                if not ticker or ticker.lower() == 'cash':
                    continue

                nom_entreprise = row.get('name')
                quantite = pd.to_numeric(clean_currency(row.get('no._of_shares')), errors='coerce')
                devise = detect_currency(row.get('holding_value', ''))
                
                ticker_pour_yfinance = ticker
                if isinstance(ticker, str):
                    ticker_propre = ticker.strip()
                    if ticker_propre.upper().startswith('TSE:'):
                        base_ticker = ticker_propre.split(':')[1].replace('.', '-')
                        ticker_pour_yfinance = base_ticker.upper() + '.TO'
                    else:
                        ticker_pour_yfinance = ticker_propre.upper()
                
                valeur = None
                if ticker_pour_yfinance:
                    try:
                        stock = yf.Ticker(ticker_pour_yfinance)
                        hist = stock.history(period="1d")
                        if not hist.empty:
                            valeur = hist['Close'].iloc[-1]
                    except Exception:
                        pass

                if valeur is None:
                    valeur = pd.to_numeric(clean_currency(row.get('price')), errors='coerce')

                if pd.isna(nom_entreprise) or pd.isna(quantite) or pd.isna(valeur):
                    continue
                
                quantite, valeur = int(quantite), float(valeur)
                
                result_cursor = conn.execute(text("SELECT id FROM titres WHERE ticker = :ticker"), {'ticker': ticker})
                result = result_cursor.fetchone()
                
                if result:
                    titre_id = int(result[0])
                else:
                    insert_titre_stmt = text("INSERT INTO titres (ticker, nom_entreprise) VALUES (:ticker, :nom)")
                    cursor = conn.execute(insert_titre_stmt, {'ticker': ticker, 'nom': nom_entreprise})
                    titre_id = int(cursor.lastrowid)

                insert_histo_stmt = text("""
                    INSERT INTO historique (titre_id, date_releve, valeur, quantite, devise) 
                    VALUES (:id, :date, :val, :qte, :devise)
                    ON DUPLICATE KEY UPDATE valeur=VALUES(valeur), quantite=VALUES(quantite), devise=VALUES(devise)
                """)
                conn.execute(insert_histo_stmt, {'id': titre_id, 'date': date_du_releve, 'val': valeur, 'qte': quantite, 'devise': devise})

            except Exception as row_error:
                logging.error(f"Erreur sur ticker {row.get('ticker')}: {row_error}")
        
        trans.commit()
except Exception as e:
    logging.error(f"Erreur majeure : {e}", exc_info=True)