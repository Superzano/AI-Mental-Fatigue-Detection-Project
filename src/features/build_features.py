import pandas as pd
import neurokit2 as nk


class FeaturesExtractor:
    def __init__(self, ecg_file_path, ecg_column_mapping, ecg_selected_columns, conditions,
                 ecg_sampling_rate, ecg_sliding_window_size, ecg_overlapping_ratio, user_list):
        """
        Initialize the FeaturesExtractor class.

        Parameters:
            ecg_file_path (str): The file path to the CSV file containing ECG data.
            ecg_column_mapping (dict): A dictionary mapping old column names to new column names for ECG data.
            ecg_selected_columns (list): A list of column names to select from the ECG DataFrame.
            conditions (list): A list of values representing the conditions to split the DataFrame.
            ecg_sampling_rate (int): The sampling rate of the ECG signal, i.e., the number of samples per second.
            ecg_sliding_window_size (float): The size of the sliding window in seconds for splitting the data.
            ecg_overlapping_ratio (float): The percentage of overlapping between sliding windows (0 to 1).
            user_list (list): A list of user IDs to extract features for.
        """
        self.ecg_file_path = ecg_file_path
        self.ecg_column_mapping = ecg_column_mapping
        self.ecg_selected_columns = ecg_selected_columns
        self.conditions = conditions
        self.ecg_sampling_rate = ecg_sampling_rate
        self.ecg_sliding_window_size = ecg_sliding_window_size
        self.ecg_overlapping_ratio = ecg_overlapping_ratio
        self.user_list = user_list
        self.df_ecg_converted_timestamp = None
        self.subset_dataframes_list = None
        self.hrv_df_condition_list = list()

    def rename_columns(self, df, column_mapping):
        """
        Returns a copy of the DataFrame with renamed columns based on a dictionary mapping.

        Parameters:
            df (pd.DataFrame): The DataFrame to modify.
            column_mapping (dict): A dictionary containing the associations between the old column names
                                   and the new column names.

        Returns:
            pd.DataFrame: A new DataFrame with the renamed columns.
        """
        return df.rename(columns=column_mapping)

    def select_columns(self, df, columns_to_select):
        """
        Returns a new DataFrame with the subset of columns specified in the input list.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            columns_to_select (list): A list of column names to select from the DataFrame.

        Returns:
            pd.DataFrame: A new DataFrame containing the subset of columns specified in the input list.
        """
        return df[columns_to_select].copy()

    def convert_timestamp_to_time(self, df, timestamp_column):
        """
        Converts the timestamp column in the DataFrame to datetime.time format and returns a new DataFrame.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            timestamp_column (str): The name of the timestamp column to convert.

        Returns:
            pd.DataFrame: A new DataFrame with the timestamp column converted to datetime.time format.
        """
        new_df = df.copy()
        new_df[timestamp_column] = pd.to_datetime(new_df[timestamp_column], errors="coerce")
        return new_df

    def split_data_by_condition(self, df, condition_column, condition_values):
        """
        Splits the input DataFrame into multiple subset DataFrames based on the specified conditions.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            condition_column (str): The name of the column to use for splitting (e.g., "Condition").
            condition_values (list): A list of values representing the conditions to split the DataFrame.

        Returns:
            list: A list of subset DataFrames, one for each condition specified in the list.
        """
        subset_dataframes = [df[df[condition_column] == value].copy() for value in condition_values]
        return subset_dataframes

    def split_dataset_into_windows(self, df, timestamp_column, window_size, overlap_ratio=0):
        """
        Splits the input DataFrame into temporal subsets using sliding windows.

        Parameters:
            df (pd.DataFrame): The input DataFrame.
            timestamp_column (str): The name of the timestamp column in the DataFrame.
            window_size (float): The size of the sliding window in seconds.
            overlap_ratio (float, optional): The percentage of overlapping between sliding windows (0 to 1).

        Returns:
            list: A list of DataFrames, each representing a temporal subset obtained using the sliding window.
        """
        df_sorted = df.sort_values(by=timestamp_column)
        overlap_points = int(window_size * overlap_ratio)
        temporal_subsets = []

        for i in range(0, len(df_sorted) - window_size + 1, window_size - overlap_points):
            temporal_subset = df_sorted.iloc[i : i + window_size]
            temporal_subsets.append(temporal_subset)

        return temporal_subsets

    def intervals_to_peaks(self, dataframe, column_name):
        """
        Convert RR-Interval values to R-peaks using NeuroKit2.

        Parameters:
            dataframe (pd.DataFrame): The input DataFrame.
            column_name (str): The name of the column containing RR-Interval values.

        Returns:
            pd.Series: A new Series containing the R-peaks obtained from the RR-Interval values.
        """
        r_peaks = nk.intervals_to_peaks(dataframe[column_name])
        return r_peaks

    def calculate_hrv_features(self, r_peaks, sampling_rate, show=False):
        """
        Calculates Heart Rate Variability (HRV) features dataframe from R-peaks array using NeuroKit2.

        Parameters:
            r_peaks (array): An array containing the locations of R-peaks in the ECG signal.
                             R-peaks represent the heart's electrical activity peaks.
            sampling_rate (int): The sampling rate of the ECG signal, i.e., the number of samples per second.
                                 It is used to convert time intervals to seconds in HRV calculations.
            show (bool, optional): A boolean to decide whether showing HRV-related plots or not. 
                                   Set to True to display HRV-related plots; False otherwise.

        Returns:
            DataFrame: A pandas DataFrame containing various HRV features computed from the R-peaks.
                       The DataFrame includes metrics such as RMSSD, SDNN, pNN50, LF, HF, etc.
                       Each row corresponds to a specific HRV feature for the entire signal or specific segments.
        """
        hrv_features_df = nk.hrv(r_peaks, sampling_rate=sampling_rate, show=show)
        return hrv_features_df

    def concatenate_dataframes(self, dataframes_list, axis=0):
        """
        Concatenates a list of DataFrames into a single DataFrame.

        Parameters:
            dataframes_list (list): A list of DataFrames to be concatenated.
            axis (int, optional): The axis along which the DataFrames will be concatenated.
                                  If axis=0, concatenates vertically (rows).
                                  If axis=1, concatenates horizontally (columns).

        Returns:
            pd.DataFrame: A new DataFrame obtained by concatenating all DataFrames in the input list.
        """
        concatenated_df = pd.concat(dataframes_list, axis=axis, ignore_index=True)
        return concatenated_df

    def run(self):
        """
        Execute the Feature Extraction Pipeline.
        """
        df_ecg = pd.read_csv(self.ecg_file_path)
        df_ecg_renamed = self.rename_columns(df_ecg, self.ecg_column_mapping)
        df_ecg_selected = self.select_columns(df_ecg_renamed, self.ecg_selected_columns)
        self.df_ecg_converted_timestamp = self.convert_timestamp_to_time(df_ecg_selected, "Timestamp")
        self.subset_dataframes_list = self.split_data_by_condition(
            self.df_ecg_converted_timestamp, "Condition", self.conditions
        )

        for df in self.subset_dataframes_list:
            hrv_df_list = list()

            for user in self.user_list:
                user_subset = df.loc[df["User"] == user]
                temporal_subsets = self.split_dataset_into_windows(
                    user_subset, "Timestamp", self.ecg_sliding_window_size, self.ecg_overlapping_ratio
                )

                for temporal_subset in temporal_subsets:
                    r_peaks = self.intervals_to_peaks(temporal_subset, "RR_Interval_ms")
                    hrv_features_df = self.calculate_hrv_features(r_peaks, sampling_rate=self.ecg_sampling_rate, show=False)

                    hrv_features_df.insert(0, "ECG_Rate_Mean", temporal_subset["HR_bpm"].mean())
                    hrv_features_df.insert(0, "Condition", df["Condition"].unique()[0])
                    hrv_features_df.insert(0, "User", user)
                    hrv_df_list.append(hrv_features_df)

            hrv_df_condition = self.concatenate_dataframes(hrv_df_list, axis=0)
            self.hrv_df_condition_list.append(hrv_df_condition)
