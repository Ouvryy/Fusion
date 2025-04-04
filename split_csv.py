import csv

def split_csv_file(input_file, output_prefix='split_result_', max_lines_per_file=50000, max_files=5):
    """
    Découpe un fichier CSV en plusieurs morceaux (avec entête) mais
    ne génère que les `max_files` premiers sous-fichiers.

    Paramètres:
        - input_file (str): Chemin vers le fichier CSV à découper.
        - output_prefix (str): Préfixe pour les sous-fichiers générés.
        - max_lines_per_file (int): Nombre maximum de lignes (hors entête) par fichier.
        - max_files (int): Nombre maximum de fichiers à générer.
    """
    with open(input_file, 'r', newline='', encoding='utf-8') as f_in:
        reader = csv.reader(f_in)

        # Lire l'entête
        header = next(reader, None)
        if header is None:
            print("Le fichier CSV est vide. Rien à découper.")
            return

        file_count = 1
        lines_in_chunk = 0
        f_out = None
        writer = None

        for row in reader:
            # Créer un nouveau fichier si nécessaire
            if lines_in_chunk == 0:
                if file_count > max_files:
                    print(f"Limite de {max_files} fichiers atteinte. Découpage arrêté.")
                    break
                output_file_name = f"{output_prefix}{file_count}.csv"
                f_out = open(output_file_name, 'w', newline='', encoding='utf-8')
                writer = csv.writer(f_out)
                writer.writerow(header)
                print(f"Création de : {output_file_name}")

            writer.writerow(row)
            lines_in_chunk += 1

            # Si on atteint le quota de lignes, on ferme le fichier et passe au suivant
            if lines_in_chunk >= max_lines_per_file:
                f_out.close()
                lines_in_chunk = 0
                file_count += 1

        # Fermer le dernier fichier s’il reste ouvert
        if f_out and not f_out.closed:
            f_out.close()

        print(f"\nDécoupage terminé. {min(file_count, max_files)} fichier(s) généré(s).")

if __name__ == "__main__":
    input_csv = "resultat_fusion_nettoyage.csv"
    split_csv_file(input_csv, output_prefix="split_", max_lines_per_file=3000, max_files=5)
