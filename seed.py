# seed.py
from datetime import date
from flask_app import app, db, Titre, Historique # Importer Historique

with app.app_context():
    # --- Suppression des anciennes données ---
    print("Suppression des anciennes données...")
    Historique.query.delete()
    Titre.query.delete()

    # --- Création des titres ---
    print("Création des titres...")
    titre1 = Titre(ticker='AAPL', nom_entreprise='Apple Inc.')
    titre2 = Titre(ticker='GOOGL', nom_entreprise='Alphabet Inc.')
    titre3 = Titre(ticker='MSFT', nom_entreprise='Microsoft Corporation')
    db.session.add_all([titre1, titre2, titre3])
    db.session.commit() # On commit ici pour que les titres aient un ID

    # --- Création des données historiques ---
    print("Création des données historiques...")
    histo1 = Historique(titre=titre1, date_releve=date(2025, 7, 25), valeur=190.5, quantite=10)
    histo2 = Historique(titre=titre1, date_releve=date(2025, 8, 1), valeur=195.0, quantite=10)
    histo3 = Historique(titre=titre2, date_releve=date(2025, 7, 25), valeur=130.2, quantite=15)
    histo4 = Historique(titre=titre2, date_releve=date(2025, 8, 1), valeur=135.8, quantite=15)
    db.session.add_all([histo1, histo2, histo3, histo4])

    # --- Validation finale ---
    db.session.commit()
    print("Les données ont été ajoutées avec succès !")