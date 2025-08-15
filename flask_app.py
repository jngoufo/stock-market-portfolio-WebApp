import os
import configparser
from datetime import date, datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_bcrypt import Bcrypt
from dotenv import load_dotenv
import locale
from collections import namedtuple
from zoneinfo import ZoneInfo

# --- Configuration du Français pour les dates ---
try:
    locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'French')
    except locale.Error:
        print("Locale 'fr_FR.UTF-8' or 'French' not found. Dates might be in English.")

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = '4b47631cf98d3e15e273993721790065' # IMPORTANT: Remplacez par votre propre clé secrète
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message = "Veuillez vous connecter pour accéder à cette page."
login_manager.login_message_category = "info"


# --- MODÈLES DE BASE DE DONNÉES ---
class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)

class Titre(db.Model):
    __tablename__ = 'titres'
    id = db.Column(db.Integer, primary_key=True)
    ticker = db.Column(db.String(20), unique=True, nullable=False)
    nom_entreprise = db.Column(db.String(100), nullable=False)
    historique = db.relationship('Historique', backref='titre', lazy=True)
    #V2.0 On rajoute les colonnes pour les données sur 52 semaines
    an_haut = db.Column(db.Float, nullable=True)
    an_bas = db.Column(db.Float, nullable=True)

class Historique(db.Model):
    __tablename__ = 'historique'
    id = db.Column(db.Integer, primary_key=True)
    titre_id = db.Column(db.Integer, db.ForeignKey('titres.id'), nullable=False)
    date_releve = db.Column(db.Date, nullable=True)
    valeur = db.Column(db.Float, nullable=False)
    quantite = db.Column(db.Float, nullable=False)
    devise = db.Column(db.String(3), nullable=False, default='USD')

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


# --- ROUTES DE CONNEXION / DÉCONNEXION ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form.get('username')).first()
        if user and bcrypt.check_password_hash(user.password_hash, request.form.get('password')):
            login_user(user)
            next_page = request.args.get('next')
            return redirect(next_page or url_for('index'))
        else:
            flash('Identifiants incorrects. Veuillez réessayer.', 'danger')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


# --- ROUTES DE L'APPLICATION PRIVÉE ---
@app.route('/')
@login_required
def index():
    try:
        les_titres = Titre.query.order_by(Titre.nom_entreprise).all()
        return render_template('index.html', titres=les_titres)
    except Exception as e:
        return f"<h1>Une erreur est survenue.</h1><p>Détails :<br>{e}</p>"

@app.route('/titre/<int:titre_id>')
@login_required
def titre_detail(titre_id):
    try:
        titre = Titre.query.get_or_404(titre_id)
        historique_valide = [h for h in titre.historique if h.date_releve]
        historique_trie = sorted(historique_valide, key=lambda h: h.date_releve)

        performance = None
        if len(historique_trie) >= 2:
            dernier_releve = historique_trie[-1]
            avant_dernier_releve = historique_trie[-2]
            if avant_dernier_releve.valeur != 0:
                variation_absolue = dernier_releve.valeur - avant_dernier_releve.valeur
                variation_pourcentage = (variation_absolue / avant_dernier_releve.valeur) * 100
                performance = {"absolue": variation_absolue, "pourcentage": variation_pourcentage}

        labels = [h.date_releve.strftime('%d %B %Y') for h in historique_trie]
        valeurs = [h.valeur for h in historique_trie]
        return render_template('titre_detail.html', titre=titre, labels=labels, valeurs=valeurs, performance=performance)
    except Exception as e:
        return f"<h1>Une erreur est survenue sur la page de détail.</h1><p>Détails :<br>{e}</p>"

@app.route('/dashboard')
@login_required
def dashboard():
    try:
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
        config.read(config_path)
        usd_to_cad_rate = config.getfloat('settings', 'usd_to_cad_rate', fallback=1.35)

        valeur_par_date_cad = {}
        historiques = Historique.query.all()
        for h in historiques:
            if not h.date_releve:
                continue
            valeur_releve = (h.valeur or 0) * (h.quantite or 0)
            valeur_releve_cad = valeur_releve * usd_to_cad_rate if h.devise == 'USD' else valeur_releve
            if h.date_releve in valeur_par_date_cad:
                valeur_par_date_cad[h.date_releve] += valeur_releve_cad
            else:
                valeur_par_date_cad[h.date_releve] = valeur_releve_cad

        dates_triees = sorted(valeur_par_date_cad.keys())
        labels = [d.strftime('%d %b %Y') for d in dates_triees]
        valeurs_totales_cad = [valeur_par_date_cad[d] for d in dates_triees]

        performance_globale = None
        if len(valeurs_totales_cad) >= 2:
            derniere_valeur = valeurs_totales_cad[-1]
            avant_derniere_valeur = valeurs_totales_cad[-2]
            if avant_derniere_valeur != 0:
                variation_absolue = derniere_valeur - avant_derniere_valeur
                variation_pourcentage = (variation_absolue / avant_derniere_valeur) * 100
                performance_globale = {"valeur_actuelle": derniere_valeur, "absolue": variation_absolue, "pourcentage": variation_pourcentage, "devise": "CAD"}

        performances_individuelles = []
        tous_les_titres = Titre.query.all()
        for titre in tous_les_titres:
            historique_valide = [h for h in titre.historique if h.date_releve]
            historique_trie = sorted(historique_valide, key=lambda h: h.date_releve)
            if len(historique_trie) >= 2:
                dernier = historique_trie[-1]
                avant_dernier = historique_trie[-2]
                if avant_dernier.valeur != 0:
                    variation_pct = ((dernier.valeur - avant_dernier.valeur) / avant_dernier.valeur) * 100
                    performances_individuelles.append({"nom": titre.nom_entreprise, "ticker": titre.ticker, "performance_pct": variation_pct})

        meilleurs_performeurs = []
        pires_performeurs = []
        if performances_individuelles:
            performances_triees = sorted(performances_individuelles, key=lambda p: p['performance_pct'])
            pires_performeurs = performances_triees[:10]
            meilleurs_performeurs = performances_triees[-10:][::-1]

        # --- v2.0 : Calcul de proximité 52 semaines ---
        top_10_haut = []
        top_10_bas = []
        titres_avec_donnees = []
        tous_les_titres = Titre.query.all()

        for titre in tous_les_titres:
            dernier_releve = db.session.query(Historique).filter_by(titre_id=titre.id).order_by(Historique.date_releve.desc()).first()
            # On vérifie que le titre a bien les données an_haut/an_bas (elles peuvent être None)
            if dernier_releve and hasattr(titre, 'an_haut') and hasattr(titre, 'an_bas'):
                titre.prix_actuel = dernier_releve.valeur
                titres_avec_donnees.append(titre)

        if titres_avec_donnees:
            for titre in titres_avec_donnees:
                if titre.an_haut and titre.an_haut > 0:
                    titre.proximite_haut_pct = (titre.prix_actuel / titre.an_haut) * 100
                if titre.an_bas and titre.an_bas > 0:
                    titre.proximite_bas_pct = (titre.prix_actuel / titre.an_bas) * 100

            top_10_haut = sorted([t for t in titres_avec_donnees if hasattr(t, 'proximite_haut_pct')], key=lambda x: x.proximite_haut_pct, reverse=True)[:10]
            top_10_bas = sorted([t for t in titres_avec_donnees if hasattr(t, 'proximite_bas_pct')], key=lambda x: x.proximite_bas_pct)[:10]

        return render_template(
            'dashboard.html',
            performance=performance_globale,
            labels=labels,
            valeurs=valeurs_totales_cad,
            meilleurs_performeurs=meilleurs_performeurs,
            pires_performeurs=pires_performeurs,
            top_10_haut=top_10_haut,
            top_10_bas=top_10_bas
        )
    except Exception as e:
        return f"<h1>Une erreur est survenue lors du calcul du dashboard.</h1><p>Détails :<br>{e}</p>"


# --- ROUTES PUBLIQUES POUR LA DÉMO ---
@app.route('/demo')
def demo_index():
    titres_demo = [
        {'ticker': 'TSLA', 'nom_entreprise': 'Tesla, Inc.', 'prix_actuel': 265.5, 'prix_precedent': 250.0, 'an_haut': 300, 'an_bas': 150},
        {'ticker': 'NVDA', 'nom_entreprise': 'NVIDIA Corporation', 'prix_actuel': 475.8, 'prix_precedent': 450.2, 'an_haut': 500, 'an_bas': 200},
        {'ticker': 'AMZN', 'nom_entreprise': 'Amazon.com, Inc.', 'prix_actuel': 128.9, 'prix_precedent': 130.1, 'an_haut': 145, 'an_bas': 85},
        {'ticker': 'AAPL', 'nom_entreprise': 'Apple Inc.', 'prix_actuel': 175.2, 'prix_precedent': 170.5, 'an_haut': 190, 'an_bas': 125},
        {'ticker': 'MSFT', 'nom_entreprise': 'Microsoft Corporation', 'prix_actuel': 325.5, 'prix_precedent': 330.0, 'an_haut': 350, 'an_bas': 220},
        {'ticker': 'GOOGL', 'nom_entreprise': 'Alphabet Inc.', 'prix_actuel': 135.0, 'prix_precedent': 134.0, 'an_haut': 140, 'an_bas': 90},
        {'ticker': 'JPM', 'nom_entreprise': 'JPMorgan Chase', 'prix_actuel': 155.1, 'prix_precedent': 152.0, 'an_haut': 160, 'an_bas': 110},
        {'ticker': 'PFE', 'nom_entreprise': 'Pfizer Inc.', 'prix_actuel': 36.8, 'prix_precedent': 36.0, 'an_haut': 55, 'an_bas': 35},
        {'ticker': 'DIS', 'nom_entreprise': 'Walt Disney Co.', 'prix_actuel': 90.1, 'prix_precedent': 88.0, 'an_haut': 120, 'an_bas': 80},
        {'ticker': 'XOM', 'nom_entreprise': 'Exxon Mobil', 'prix_actuel': 110.0, 'prix_precedent': 108.0, 'an_haut': 120, 'an_bas': 85},
        {'ticker': 'BAC', 'nom_entreprise': 'Bank of America', 'prix_actuel': 30.0, 'prix_precedent': 29.0, 'an_haut': 38, 'an_bas': 28},
    ]
    for titre in titres_demo:
        titre['proximite_haut_pct'] = (titre['prix_actuel'] / titre['an_haut']) * 100 if titre['an_haut'] else -1
        titre['proximite_bas_pct'] = (titre['prix_actuel'] / titre['an_bas']) * 100 if titre['an_bas'] else float('inf')
        if titre['prix_precedent'] > 0:
            titre['performance_pct'] = ((titre['prix_actuel'] - titre['prix_precedent']) / titre['prix_precedent']) * 100
        else:
            titre['performance_pct'] = 0
    top_10_haut = sorted([t for t in titres_demo], key=lambda x: x['proximite_haut_pct'], reverse=True)[:10]
    top_10_bas = sorted([t for t in titres_demo], key=lambda x: x['proximite_bas_pct'])[:10]
    performances_triees = sorted(titres_demo, key=lambda x: x['performance_pct'])
    pires_performeurs_semaine = performances_triees[:10]
    meilleurs_performeurs_semaine = performances_triees[-10:][::-1]
    return render_template('demo_index.html', titres=titres_demo, top_10_haut=top_10_haut, top_10_bas=top_10_bas, meilleurs_performeurs_semaine=meilleurs_performeurs_semaine, pires_performeurs_semaine=pires_performeurs_semaine)

@app.route('/demo/titre/<string:ticker>')
def demo_titre_detail(ticker):
    class TitreFactice:
        def __init__(self, ticker, nom_entreprise, historique, an_haut=None, an_bas=None):
            self.ticker = ticker
            self.nom_entreprise = nom_entreprise
            self.historique = historique
            self.an_haut = an_haut
            self.an_bas = an_bas
    HistoSimule = namedtuple('HistoSimule', ['date_releve', 'valeur', 'quantite', 'devise'])
    today = datetime.now()
    titres_data = {
        'TSLA': TitreFactice('TSLA', 'Tesla, Inc.', [HistoSimule(today - timedelta(days=7), 250.0, 10, 'USD'), HistoSimule(today, 265.5, 10, 'USD')], an_haut=300, an_bas=150),
        'NVDA': TitreFactice('NVDA', 'NVIDIA Corporation', [HistoSimule(today - timedelta(days=7), 450.2, 5, 'USD'), HistoSimule(today, 475.8, 5, 'USD')], an_haut=500, an_bas=200),
        'AMZN': TitreFactice('AMZN', 'Amazon.com, Inc.', [HistoSimule(today - timedelta(days=7), 130.1, 15, 'USD'), HistoSimule(today, 128.9, 15, 'USD')], an_haut=145, an_bas=85),
        'AAPL': TitreFactice('AAPL', 'Apple Inc.', [HistoSimule(today - timedelta(days=7), 170.5, 12, 'USD'), HistoSimule(today, 175.2, 12, 'USD')], an_haut=190, an_bas=125),
        'MSFT': TitreFactice('MSFT', 'Microsoft Corporation', [HistoSimule(today - timedelta(days=7), 330.0, 8, 'USD'), HistoSimule(today, 325.5, 8, 'USD')], an_haut=350, an_bas=220),
        'GOOGL': TitreFactice('GOOGL', 'Alphabet Inc.', [HistoSimule(today - timedelta(days=7), 134.0, 10, 'USD'), HistoSimule(today, 135.0, 10, 'USD')], an_haut=140, an_bas=90),
        'JPM': TitreFactice('JPM', 'JPMorgan Chase', [HistoSimule(today - timedelta(days=7), 152.0, 20, 'USD'), HistoSimule(today, 155.1, 20, 'USD')], an_haut=160, an_bas=110),
        'PFE': TitreFactice('PFE', 'Pfizer Inc.', [HistoSimule(today - timedelta(days=7), 36.0, 50, 'USD'), HistoSimule(today, 36.8, 50, 'USD')], an_haut=55, an_bas=35),
        'DIS': TitreFactice('DIS', 'Walt Disney Co.', [HistoSimule(today - timedelta(days=7), 88.0, 25, 'USD'), HistoSimule(today, 90.1, 25, 'USD')], an_haut=120, an_bas=80),
        'XOM': TitreFactice('XOM', 'Exxon Mobil', [HistoSimule(today - timedelta(days=7), 108.0, 18, 'USD'), HistoSimule(today, 110.0, 18, 'USD')], an_haut=120, an_bas=85),
        'BAC': TitreFactice('BAC', 'Bank of America', [HistoSimule(today - timedelta(days=7), 29.0, 60, 'USD'), HistoSimule(today, 30.0, 60, 'USD')], an_haut=38, an_bas=28)
    }
    titre = titres_data.get(ticker)
    if not titre:
        return f"Données de démo non trouvées pour le ticker : {ticker}", 404
    historique_trie = sorted(titre.historique, key=lambda h: h.date_releve)
    labels = [h.date_releve.strftime('%d %b %Y') for h in historique_trie]
    valeurs = [h.valeur for h in historique_trie]
    performance = None
    if len(historique_trie) >= 2:
        dernier, avant_dernier = historique_trie[-1], historique_trie[-2]
        if avant_dernier.valeur != 0:
            performance = {"absolue": dernier.valeur - avant_dernier.valeur, "pourcentage": ((dernier.valeur - avant_dernier.valeur) / avant_dernier.valeur) * 100}
    return render_template('titre_detail.html', titre=titre, labels=labels, valeurs=valeurs, performance=performance)