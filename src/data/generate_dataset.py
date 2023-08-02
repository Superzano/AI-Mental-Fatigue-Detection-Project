import os
import csv

class DataProcessor:
    def __init__(self, input_folder):
        self.input_folder = input_folder

    def read_data(self, file_path, delimiter):
        with open(file_path, "r") as f:
            reader = csv.reader(f, delimiter=delimiter)
            data = [row for row in reader]
        return data

    def get_user_folders(self, last_30_users):
        user_folders = [f for f in os.listdir(self.input_folder) if f.startswith("user")]
        if last_30_users:
            user_folders = sorted(user_folders, key=lambda x: int(x[4:]))[29:60]
        else:
            user_folders = sorted(user_folders, key=lambda x: int(x[4:]))[:30]
        return user_folders

    def process_data(self, data_type, delimiter, last_30_users):
        user_folders = self.get_user_folders(last_30_users)
        dataset = []

        for user_folder in user_folders:
            print(f"Getting data for {user_folder} ...")
            data_files = [f for f in os.listdir(os.path.join(self.input_folder, user_folder, data_type)) if f.endswith(".txt")]

            for data_file in data_files:
                condition = data_file.replace(f"{user_folder}_", "").replace(".txt", "")
                file_path = os.path.join(self.input_folder, user_folder, data_type, data_file)
                data = self.read_data(file_path, delimiter)
                for row in data:
                    sample = [user_folder, condition] + row
                    dataset.append(sample)
        return dataset

    def write_data(self, data, output_file, data_type):
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            if data_type == "ECG_Analysis":
                writer.writerow(["User", "Condition", "Timestamp", "Column2", "Column3", "Column4", "Column5", "Column6"])
            elif data_type == "EDA-EMG":
                writer.writerow(["User", "Condition", "Timestamp", "Column2", "Column3", "Column4"])
            writer.writerows(data)

    def save_dataframe(self, dataframe, file_name, extension, output_path):
        # Create the path if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        output_file = os.path.join(output_path, file_name)

        if extension == ".csv":
            output_file = output_file + extension
            dataframe.to_csv(output_file, index=False)
        elif extension == ".xlsx":
            output_file = output_file + extension
            dataframe.to_excel(output_file, index=False)
        else:
            raise ValueError("Extension not supported. Use '.csv' o '.xlsx'.")