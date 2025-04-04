#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import pandas as pd
import numpy as np
import re
import os

# Chemins des fichiers
fichier_bnb = "Test.xlsx - BNB.csv"
fichier_ademe = "Test.xlsx - Ademe.csv"
fichier_sortie = "C:\\Users\\User\\Documents\\ProjetIA\\NettoyageEtTrie\\df_consolide.csv"


def analyser_structure_fichiers():
    """
    Analyse la structure des fichiers CSV pour comprendre leur format et leurs colonnes.
    
    Returns:
        tuple: Un tuple contenant les en-têtes des deux fichiers
    """
    # Lire les en-têtes des fichiers
    with open(fichier_bnb, 'r') as f:
        entete_bnb = f.readline().strip().split(',')
    
    with open(fichier_ademe, 'r') as f:
        entete_ademe = f.readline().strip().split(',')
    
    print(f"Nombre de colonnes dans BNB: {len(entete_bnb)}")
    print(f"Nombre de colonnes dans Ademe: {len(entete_ademe)}")
    
    # Vérifier si Surface_habitable_logement existe dans les en-têtes
    if 'surface_habitable_logement' in entete_bnb or 'Surface_habitable_logement' in entete_bnb:
        print("La colonne Surface_habitable_logement existe dans BNB")
        idx = entete_bnb.index('surface_habitable_logement' if 'surface_habitable_logement' in entete_bnb else 'Surface_habitable_logement')
        print(f"Position dans BNB: {idx}")
    else:
        print("La colonne Surface_habitable_logement n'existe pas dans BNB")
    
    if 'Surface_habitable_logement' in entete_ademe:
        print("La colonne Surface_habitable_logement existe dans Ademe")
        idx = entete_ademe.index('Surface_habitable_logement')
        print(f"Position dans Ademe: {idx}")
    else:
        print("La colonne Surface_habitable_logement n'existe pas dans Ademe")
    
    return entete_bnb, entete_ademe

def nettoyer_surface_habitable(df, nom_colonne='Surface_habitable_logement'):
    """
    Nettoie la colonne Surface_habitable_logement en convertissant les valeurs en nombres
    et en traitant les valeurs aberrantes.
    
    Args:
        df (pandas.DataFrame): Le DataFrame contenant la colonne à nettoyer
        nom_colonne (str): Le nom de la colonne à nettoyer
        
    Returns:
        pandas.DataFrame: Le DataFrame avec la colonne nettoyée
    """
    # Créer une copie pour éviter les avertissements de pandas
    df_clean = df.copy()
    
    # Vérifier si la colonne existe
    if nom_colonne not in df_clean.columns:
        print(f"La colonne {nom_colonne} n'existe pas dans le DataFrame")
        # Créer une colonne vide si elle n'existe pas
        df_clean[nom_colonne] = np.nan
        return df_clean
    
    # Fonction pour convertir les valeurs en nombres
    def convertir_en_nombre(valeur):
        if pd.isna(valeur):
            return np.nan
        
        # Si c'est déjà un nombre, le retourner
        if isinstance(valeur, (int, float)):
            return valeur
        
        # Convertir en chaîne de caractères
        valeur_str = str(valeur).strip()
        
        # Vérifier si la valeur ressemble à des coordonnées géographiques
        if re.search(r'\d+\.\d+\s+\d+\.\d+', valeur_str):
            return np.nan
        
        # Remplacer les virgules par des points (format français)
        valeur_str = valeur_str.replace(',', '.')
        
        # Remplacer 'O' ou autres caractères non numériques par NaN
        if valeur_str.upper() == 'O' or not re.search(r'\d', valeur_str):
            return np.nan
        
        # Essayer de convertir en nombre
        try:
            return float(valeur_str)
        except ValueError:
            # Si la conversion échoue, extraire les chiffres si possible
            chiffres = re.findall(r'\d+\.?\d*', valeur_str)
            if chiffres:
                try:
                    return float(chiffres[0])
                except ValueError:
                    return np.nan
            return np.nan
    
    # Appliquer la fonction de conversion à la colonne
    df_clean[nom_colonne] = df_clean[nom_colonne].apply(convertir_en_nombre)
    
    # Filtrer les valeurs aberrantes (surfaces trop petites ou trop grandes)
    # Considérer comme aberrantes les surfaces < 5 m² ou > 1000 m²
    mask_aberrantes = (df_clean[nom_colonne] < 5) | (df_clean[nom_colonne] > 1000)
    df_clean.loc[mask_aberrantes, nom_colonne] = np.nan
    
    return df_clean

def extraire_lignes_point(fichier_csv):
    """
    Extrait les lignes commençant par "POINT" du fichier BNB.csv et crée un nouveau DataFrame.
    
    Args:
        fichier_csv (str): Chemin du fichier CSV
        
    Returns:
        pandas.DataFrame: DataFrame contenant uniquement les lignes commençant par "POINT"
    """
    # Lire le fichier ligne par ligne
    lignes = []
    entete = None
    
    with open(fichier_csv, 'r') as f:
        entete = f.readline().strip()
        for ligne in f:
            if ligne.startswith('POINT'):
                lignes.append(ligne.strip())
    
    # Créer un nouveau fichier temporaire avec l'en-tête et les lignes extraites
    fichier_temp = "bnb_point_only.csv"
    with open(fichier_temp, 'w') as f:
        f.write(entete + '\n')
        for ligne in lignes:
            f.write(ligne + '\n')
    
    # Lire le fichier temporaire avec pandas
    df = pd.read_csv(fichier_temp, low_memory=False)
    
    return df

def identifier_cles_fusion(df_bnb, df_ademe):
    """
    Identifie les clés de fusion potentielles entre les deux DataFrames.
    
    Args:
        df_bnb (pandas.DataFrame): Le DataFrame BNB
        df_ademe (pandas.DataFrame): Le DataFrame Ademe
        
    Returns:
        list: Liste des colonnes communes qui peuvent servir de clés de fusion
    """
    # Obtenir les colonnes communes
    colonnes_communes = set(df_bnb.columns).intersection(set(df_ademe.columns))
    print(f"Colonnes communes: {colonnes_communes}")
    
    # Colonnes prioritaires pour la fusion (si elles existent dans les deux DataFrames)
    cles_prioritaires = ['Identifiant_BAN', 'libelle_adresse', 'code_postal', 'code_commune_insee']
    
    # Filtrer les clés prioritaires qui existent dans les deux DataFrames
    cles_fusion = [col for col in cles_prioritaires if col in colonnes_communes]
    
    # Si aucune clé prioritaire n'est trouvée, utiliser toutes les colonnes communes
    if not cles_fusion and colonnes_communes:
        cles_fusion = list(colonnes_communes)
    
    return cles_fusion

def fusionner_dataframes(df_bnb, df_ademe, cles_fusion):
    """
    Fusionne les deux DataFrames en utilisant les clés de fusion spécifiées.
    
    Args:
        df_bnb (pandas.DataFrame): Le DataFrame BNB nettoyé
        df_ademe (pandas.DataFrame): Le DataFrame Ademe nettoyé
        cles_fusion (list): Liste des colonnes à utiliser comme clés de fusion
        
    Returns:
        pandas.DataFrame: Le DataFrame résultant de la fusion
    """
    # Vérifier si les clés de fusion existent
    if not cles_fusion:
        print("Aucune clé de fusion identifiée, création d'un DataFrame combiné sans jointure")
        # Combiner les DataFrames sans jointure
        df_combine = pd.concat([df_bnb, df_ademe], ignore_index=True)
        return df_combine
    
    # Préfixer les colonnes pour éviter les conflits
    df_bnb_prefixe = df_bnb.add_prefix('bnb_')
    df_ademe_prefixe = df_ademe.add_prefix('ademe_')
    
    # Renommer les clés de fusion pour qu'elles soient identiques dans les deux DataFrames
    for cle in cles_fusion:
        df_bnb_prefixe = df_bnb_prefixe.rename(columns={f'bnb_{cle}': cle})
        df_ademe_prefixe = df_ademe_prefixe.rename(columns={f'ademe_{cle}': cle})
    
    # Fusionner les DataFrames
    df_fusionne = pd.merge(
        df_bnb_prefixe, 
        df_ademe_prefixe, 
        on=cles_fusion, 
        how='outer',  # Utiliser 'outer' pour conserver toutes les lignes des deux DataFrames
        indicator=True  # Ajouter une colonne indiquant la source des données
    )
    
    return df_fusionne

def consolider_surface_habitable(df_fusionne):
    """
    Consolide la colonne Surface_habitable_logement à partir des deux sources.
    
    Args:
        df_fusionne (pandas.DataFrame): Le DataFrame fusionné
        
    Returns:
        pandas.DataFrame: Le DataFrame avec la colonne Surface_habitable_logement consolidée
    """
    # Créer une copie pour éviter les avertissements de pandas
    df_consolide = df_fusionne.copy()
    
    # Identifier les colonnes de surface habitable dans le DataFrame fusionné
    colonnes_surface = [col for col in df_consolide.columns if 'surface_habitable_logement' in col.lower()]
    print(f"Colonnes de surface habitable trouvées: {colonnes_surface}")
    
    if not colonnes_surface:
        print("Aucune colonne de surface habitable trouvée dans le DataFrame fusionné")
        # Créer une colonne vide
        df_consolide['Surface_habitable_logement'] = np.nan
        return df_consolide
    
    # Créer une nouvelle colonne Surface_habitable_logement consolidée
    # Priorité à la valeur Ademe si elle existe et n'est pas NaN
    col_ademe = next((col for col in colonnes_surface if 'ademe' in col.lower()), None)
    col_bnb = next((col for col in colonnes_surface if 'bnb' in col.lower()), None)
    
    if col_ademe and col_bnb:
        df_consolide['Surface_habitable_logement'] = df_consolide[col_ademe].combine_first(df_consolide[col_bnb])
    elif col_ademe:
        df_consolide['Surface_habitable_logement'] = df_consolide[col_ademe]
    elif col_bnb:
        df_consolide['Surface_habitable_logement'] = df_consolide[col_bnb]
    else:
        df_consolide['Surface_habitable_logement'] = np.nan
    
    return df_consolide

def main():
    """
    Fonction principale qui exécute le processus de nettoyage et de fusion.
    """
    print("Début du processus de nettoyage et de fusion...")
    
    # Analyser la structure des fichiers
    print("Analyse de la structure des fichiers...")
    entete_bnb, entete_ademe = analyser_structure_fichiers()
    
    # Extraire les lignes commençant par "POINT" du fichier BNB
    print("Extraction des lignes commençant par 'POINT' du fichier BNB...")
    df_bnb = extraire_lignes_point(fichier_bnb)
    print(f"Nombre de lignes extraites: {len(df_bnb)}")
    
    # Charger le fichier Ademe
    print(f"Chargement du fichier Ademe: {fichier_ademe}")
    df_ademe = pd.read_csv(fichier_ademe, low_memory=False)
    
    # Afficher les premières lignes des DataFrames
    print("\nPremières lignes du DataFrame BNB:")
    print(df_bnb.head(2))
    print("\nPremières lignes du DataFrame Ademe:")
    print(df_ademe.head(2))
    
    # Nettoyer la colonne Surface_habitable_logement dans les deux DataFrames
    print("\nNettoyage de la colonne Surface_habitable_logement dans le fichier BNB...")
    # Vérifier si la colonne existe dans BNB
    col_surface_bnb = next((col for col in df_bnb.columns if 'surface_habitable_logement' in col.lower()), None)
    if col_surface_bnb:
        df_bnb_nettoye = nettoyer_surface_habitable(df_bnb, col_surface_bnb)
    else:
        print("Aucune colonne de surface habitable trouvée dans BNB")
        df_bnb_nettoye = df_bnb.copy()
        df_bnb_nettoye['Surface_habitable_logement'] = np.nan
    
    print("Nettoyage de la colonne Surface_habitable_logement dans le fichier Ademe...")
    df_ademe_nettoye = nettoyer_surface_habitable(df_ademe)
    
    # Identifier les clés de fusion
    print("\nIdentification des clés de fusion...")
    cles_fusion = identifier_cles_fusion(df_bnb_nettoye, df_ademe_nettoye)
    print(f"Clés de fusion identifiées: {cles_fusion}")
    
    # Fusionner les DataFrames
    print("\nFusion des DataFrames...")
    df_fusionne = fusionner_dataframes(df_bnb_nettoye, df_ademe_nettoye, cles_fusion)
    
    if df_fusionne is None:
        print("La fusion a échoué")
        return
    
    # Consolider la colonne Surface_habitable_logement
    print("\nConsolidation de la colonne Surface_habitable_logement...")
    df_consolide = consolider_surface_habitable(df_fusionne)
    
    # Enregistrer le résultat
    print(f"\nEnregistrement du résultat dans: {fichier_sortie}")
    df_consolide.to_csv(fichier_sortie, index=False)
    
    # Afficher quelques statistiques sur la colonne Surface_habitable_logement
    print("\nStatistiques sur la colonne Surface_habitable_logement:")
    if 'Surface_habitable_logement' in df_consolide.columns:
        print(f"Nombre total de lignes: {len(df_consolide)}")
        print(f"Nombre de valeurs non-NaN: {df_consolide['Surface_habitable_logement'].count()}")
        if df_consolide['Surface_habitable_logement'].count() > 0:
            print(f"Valeur minimale: {df_consolide['Surface_habitable_logement'].min()}")
            print(f"Valeur maximale: {df_consolide['Surface_habitable_logement'].max()}")
            print(f"Valeur moyenne: {df_consolide['Surface_habitable_logement'].mean()}")
            print(f"Écart-type: {df_consolide['Surface_habitable_logement'].std()}")
        else:
            print("Aucune valeur numérique disponible pour calculer les statistiques")
    else:
        print("La colonne Surface_habitable_logement n'existe pas dans le DataFrame consolidé")
    
    print("\nProcessus de nettoyage et de fusion terminé avec succès!")

if __name__ == "__main__":
    main()
