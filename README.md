# Mon Projet
# Nettoyage et fusion de données BNB / Ademe

Ce script Python permet de traiter, nettoyer, fusionner et consolider les données issues de deux fichiers CSV provenant de sources différentes (BNB et Ademe). Il est conçu pour harmoniser notamment la colonne `Surface_habitable_logement`, puis exporter un fichier de données propre prêt à être exploité pour des analyses ultérieures.

## Objectifs

- Extraire les lignes pertinentes du fichier BNB (lignes commençant par `POINT`)
- Nettoyer les colonnes `Surface_habitable_logement` dans les deux jeux de données
- Identifier automatiquement les clés de fusion communes
- Fusionner les deux DataFrames tout en conservant la traçabilité des sources
- Consolider les informations de surface habitable
- Générer un fichier CSV final consolidé

## Fonctionnalités clés

- Gestion automatique des valeurs aberrantes (< 5 m² ou > 1000 m²)
- Harmonisation du format numérique (ex. virgules/français → points)
- Détection automatique de colonnes de fusion
- Fusion intelligente avec préfixes pour distinguer les colonnes de chaque source
- Consolidation prioritaire des données Ademe en cas de doublon
- Export CSV final prêt à l’usage

## Structure du script

Le script contient les fonctions suivantes :

- `analyser_structure_fichiers()` : Analyse des en-têtes CSV
- `nettoyer_surface_habitable(df)` : Nettoyage des valeurs de surface
- `extraire_lignes_point(fichier_csv)` : Extraction des lignes pertinentes de BNB
- `identifier_cles_fusion(df1, df2)` : Identification automatique des clés de jointure
- `fusionner_dataframes(df1, df2, cles)` : Fusion des deux sources de données
- `consolider_surface_habitable(df)` : Création d'une colonne consolidée
- `main()` : Exécution complète de la chaîne de traitement


## Prérequis

- Python 3.6+
- Bibliothèques Python :
  - `pandas`
  - `numpy`
  - `re` (standard)
  - `os` (standard)

Vous pouvez installer les dépendances nécessaires via :

```bash
pip install pandas numpy
python solution_nettoyage_fusion_corrige.py
