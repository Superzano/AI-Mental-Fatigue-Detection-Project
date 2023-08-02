# -*- coding: utf-8 -*-
import click
import os
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import sys
sys.path.append(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), 'features'))
from generate_dataset import DataProcessor
from build_features import FeaturesExtractor
from config import (
    ecg_sampling_rate,
    ecg_sliding_window_size,
    ecg_overlapping_ratio,
    ecg_column_mapping,
    ecg_selected_columns,
    conditions,
    ecg_first_30_users_file_path,
    user_list,
)


@click.command()
@click.option('-i', '--input_filepath', type=click.Path(exists=True), required=True, help='Path to the input folder containing raw data.')
@click.option('-o', '--output_filepath', type=click.Path(), required=True, help='Path to the output folder where processed data will be saved.')
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    
    #input_filepath = "data/raw/dataset_luca_cornia"
    interim_filepath = "data/interim"
    #processed_filepath = "data/processed"
    
    logger.info('Creating an instance of DataProcessor ...')
    data_processor = DataProcessor(input_filepath)

    logger.info('Making interim ECG dataset for first 30 users from raw data ...')
    interim_ecg_first_30_users_file = interim_filepath + "/dataset_ecg_first_30_users.csv"
    dataset = data_processor.process_data("ECG_Analysis", delimiter=",", last_30_users=False)
    data_processor.write_data(dataset, interim_ecg_first_30_users_file, "ECG_Analysis")

    logger.info('Making interim ECG dataset for last 30 users from raw data ...')
    interim_ecg_last_30_users_file = interim_filepath + "/dataset_ecg_last_30_users.csv"
    dataset = data_processor.process_data("ECG_Analysis", delimiter=",", last_30_users=True)
    data_processor.write_data(dataset, interim_ecg_last_30_users_file, "ECG_Analysis")

    logger.info('Making interim EDA-EMG dataset for first 30 users from raw data ...')
    interim_eda_emg_file = interim_filepath + "/dataset_eda_emg.csv"
    dataset = data_processor.process_data("EDA-EMG", delimiter="\t", last_30_users=False)
    data_processor.write_data(dataset, interim_eda_emg_file, "EDA-EMG")
    
    logger.info('Creating an instance of FeaturesExtractor ...')
    extractor = FeaturesExtractor(
        ecg_file_path=ecg_first_30_users_file_path,
        ecg_column_mapping=ecg_column_mapping,
        ecg_selected_columns=ecg_selected_columns,
        conditions=conditions,
        ecg_sampling_rate=ecg_sampling_rate,
        ecg_sliding_window_size=ecg_sliding_window_size,
        ecg_overlapping_ratio=ecg_overlapping_ratio,
        user_list=user_list
    )

    logger.info('Making final ECG Features dataset for first 30 users from interim data ...')
    extractor.run()

    hrv_df_condition_list = extractor.hrv_df_condition_list
    
    for i, df in enumerate(hrv_df_condition_list):
        file_name = conditions[i]
        data_processor.save_dataframe(df, file_name, ".csv", output_filepath)
    


if __name__ == '__main__':
    
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
