from flask_app import app, db

with app.app_context():
    print("Création des tables (si elles n'existent pas)...")
    db.create_all()
    print("Opération terminée.")