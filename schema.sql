CREATE TABLE titres (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    nom_entreprise VARCHAR(100) NOT NULL
);

CREATE TABLE historique (
    id INT AUTO_INCREMENT PRIMARY KEY,
    titre_id INT NOT NULL,
    date_releve DATE NOT NULL,
    valeur FLOAT NOT NULL,
    quantite INT NOT NULL,
    FOREIGN KEY (titre_id) REFERENCES titres(id)
);