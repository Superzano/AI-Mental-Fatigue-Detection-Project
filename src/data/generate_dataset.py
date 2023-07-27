import os
import csv

def process_data(input_folder, data_type, output_file, delimiter, last_30_users):
    # Ottieni la lista di cartelle "user" all'interno della directory input_folder
    user_folders = [f for f in os.listdir(input_folder) if f.startswith("user")]
    
    # Filtra solo i primi 30 utenti ([user0 a user29]) o gli ultimi 30 utenti ([user30 a user59)
    if last_30_users:
        user_folders = sorted(user_folders, key=lambda x: int(x[4:]))[29:60]
    else:
        user_folders = sorted(user_folders, key=lambda x: int(x[4:]))[:30]
    
    # Lista per il dataset finale
    dataset = []
    
    # Loop attraverso le cartelle "user"
    for user_folder in user_folders:
        
        print(f"Getting data for {user_folder} ...")
        
        # Ottieni la lista dei file .txt nella cartella specificata (data_type)
        data_files = [f for f in os.listdir(os.path.join(input_folder, user_folder, data_type)) if f.endswith(".txt")]
        
        # Loop attraverso i file .txt
        for data_file in data_files:
            # Ricava il tipo di condizione sperimentale dal nome del file
            condition = data_file.replace(f"{user_folder}_", "").replace(".txt", "")
            
            # Leggi i dati dal file .txt e aggiungi le informazioni utente e condizione
            with open(os.path.join(input_folder, user_folder, data_type, data_file), "r") as f:
                reader = csv.reader(f, delimiter=delimiter)
                for row in reader:
                    sample = [user_folder, condition] + row
                    dataset.append(sample)
    
    # Scrivi il dataset finale in un file CSV
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        if data_type == "ECG_Analysis":
            writer.writerow(["User", "Condition", "Timestamp", "Column2", "Column3", "Column4", "Column5", "Column6"])
        elif data_type == "EDA-EMG":
            writer.writerow(["User", "Condition", "Timestamp", "Column2", "Column3", "Column4"])
        writer.writerows(dataset)


if __name__ == "__main__":
    
    # Input folder dove sono presenti le cartelle "user"
    input_folder = "../../data/raw/dataset_luca_cornia"
    
    # Output file per il dataset ECG_Analysis
    output_ecg_first_30_users_file = "../../data/processed/dataset_ecg_first_30_users.csv"
    
    # Output file per il dataset ECG_Analysis
    output_ecg_last_30_users_file = "../../data/processed/dataset_ecg_last_30_users.csv"

    # Output file per il dataset EDA-EMG
    output_eda_emg_file = "../../data/processed/dataset_eda_emg.csv"

    # Genera il dataset per ECG_Analysis (first 30 users)
    process_data(input_folder, "ECG_Analysis", output_ecg_first_30_users_file, delimiter=",", last_30_users=False)
    
    # Genera il dataset per ECG_Analysis (last 30 users)
    process_data(input_folder, "ECG_Analysis", output_ecg_last_30_users_file, delimiter=",", last_30_users=True)

    # Genera il dataset per EDA-EMG (only first 30 users have EDA and EMG data)
    process_data(input_folder, "EDA-EMG", output_eda_emg_file, delimiter="\t", last_30_users=False)