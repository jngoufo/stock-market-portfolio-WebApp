import getpass
from flask_app import app, db, User, bcrypt

def create_admin_user():
    """Crée un utilisateur administrateur."""
    with app.app_context():
        # Demande les informations à l'utilisateur
        username = input("Entrez le nom d'utilisateur souhaité : ")

        # Vérifie si l'utilisateur existe déjà
        if User.query.filter_by(username=username).first():
            print(f"L'utilisateur '{username}' existe déjà.")
            return

        # Demande le mot de passe de manière sécurisée (il ne s'affichera pas)
        password = getpass.getpass("Entrez le mot de passe souhaité : ")

        # Crypte le mot de passe
        hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')

        # Crée la nouvelle instance utilisateur
        new_user = User(username=username, password_hash=hashed_password)

        # Ajoute et sauvegarde l'utilisateur dans la base de données
        db.session.add(new_user)
        db.session.commit()

        print(f"L'utilisateur '{username}' a été créé avec succès !")

if __name__ == '__main__':
    create_admin_user()