import os
import pandas as pd
import numpy as np


def introduce_data_issues(df):
    df.loc[df.sample(frac=0.1).index, 'Tool wear [min]'] = 'error'
    df.loc[df.sample(frac=0.1).index, 'Air temperature [K]'] = np.nan
    df.loc[df.sample(frac=0.1).index, 'Process temperature [K]'] = 'France'
    df.loc[df.sample(frac=0.1).index, 'Rotational speed [rpm]'] = df.loc[df.sample(frac=0.1).index, 'Process temperature [K]'] * -1
    df.loc[df.sample(frac=0.1).index, 'Tool wear [min]'] = df.loc[df.sample(frac=0.1).index, 'Process temperature [K]'] * 2
    df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] = df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] + np.random.normal(0, 5, size=len(df.sample(frac=0.1))) 
    # large_noise = np.random.normal(1000, 200, size=len(df.sample(frac=0.1)))
    # df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] += large_noise
    df = pd.concat([df, df.sample(frac=0.05)])
    return df

def split_and_save_data(df, output_folder, num_files):

    os.makedirs(output_folder, exist_ok=True)
    chunk_size = len(df) // num_files

    for i in range(num_files):
        start_index = i * chunk_size
        end_index = start_index + chunk_size if i < num_files - 1 else None
        split_df = df.iloc[start_index:end_index]
        split_df.to_csv(os.path.join(output_folder, f'data_split_{i}.csv'), index=False)

if __name__ == "__main__":

    main_dataset_path = 'data/onlyfeatures.csv'
    main_df = pd.read_csv(main_dataset_path)
    main_df_with_issues = introduce_data_issues(main_df)
    output_folder = 'raw-data'
    num_files_to_generate = 25
    split_and_save_data(main_df_with_issues, output_folder, num_files_to_generate)

