'''
General Features Configurations
'''

# Splitting dataset
conditions = ["user_cognitive-fatigue", "user_combo-fatigue", "user_physical-fatigue", "user_rest"]
user_list = [f"user{i}" for i in range(30)]

'''
ECG Features Configurations
'''

# ECG sample frequency (Hz)
# Probably the 1000 Hz refers to the raw ECG sample rate. The RR-Interval values in the raw data are probably computed over 1 Hz
ecg_sampling_rate = 1000

# Sliding window in seconds and overlapping ration
ecg_sliding_window_size = 150  
ecg_overlapping_ratio = 0  

# Rename columns
ecg_column_mapping = {
    "Column2": "HR_bpm",
    "Column3": "RR_Interval_1024",
    "Column4": "Exadecimal1",
    "Column5": "Exadecimal2",
    "Column6": "RR_Interval_ms"
}

# Select columns of interest
ecg_selected_columns = ["User", "Condition", "Timestamp", "HR_bpm", "RR_Interval_ms"]

# Dataset path
ecg_first_30_users_file_path = "data/interim/dataset_ecg_first_30_users.csv"

# Data Structures
hrv_df_condition_list = list()


'''
EDA Features Configurations
'''


'''
EMG Features Configurations
'''