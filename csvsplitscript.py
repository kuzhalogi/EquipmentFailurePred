import os
import pandas as pd
import numpy as np


# def introduce_data_issues(df):
#     df.loc[df.sample(frac=0.1).index, 'Tool wear [min]'] = 'error'
#     df.loc[df.sample(frac=0.1).index, 'Air temperature [K]'] = np.nan
#     df.loc[df.sample(frac=0.1).index, 'Process temperature [K]'] = 'France'
#     df.loc[df.sample(frac=0.1).index, 'Rotational speed [rpm]'] = df.loc[df.sample(frac=0.1).index, 'Process temperature [K]'] * -1
#     df.loc[df.sample(frac=0.1).index, 'Tool wear [min]'] = df.loc[df.sample(frac=0.1).index, 'Process temperature [K]'] * 2
#     df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] = df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] + np.random.normal(0, 5, size=len(df.sample(frac=0.1))) 
#     # large_noise = np.random.normal(1000, 200, size=len(df.sample(frac=0.1)))
#     # df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] += large_noise
#     df = pd.concat([df, df.sample(frac=0.05)])
#     return df

def introduce_data_issues(df):
    # Cast columns to 'object' dtype before introducing incompatible data types
    df['Rotational speed [rpm]'] = df['Rotational speed [rpm]'].astype('object')
    df['Air temperature [K]'] = df['Air temperature [K]'].astype('object')
    df['Process temperature [K]'] = df['Process temperature [K]'].astype('object')
    df['Tool wear [min]'] = df['Tool wear [min]'].astype('object')  # Cast Tool wear to object dtype
    
    # 1. Replace valid numeric entries in "Tool wear [min]" with 'error'
    df.loc[df.sample(frac=0.1).index, 'Tool wear [min]'] = 'error'

    # 2. Introduce NaN values in "Air temperature [K]"
    df.loc[df.sample(frac=0.1).index, 'Air temperature [K]'] = np.nan

    # 3. Replace numeric entries in "Process temperature [K]" with a string
    df.loc[df.sample(frac=0.1).index, 'Process temperature [K]'] = 'France'

    # 4. Replace "Rotational speed [rpm]" with negative values
    df.loc[df.sample(frac=0.1).index, 'Rotational speed [rpm]'] = -5000

    # 5. Replace "Torque [Nm]" with random invalid large noise
    df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] += 1000

    # 6. Add duplicates to the dataset
    df = pd.concat([df, df.sample(frac=0.05)], ignore_index=True)

    # 7. Introduce invalid "Product ID" values
    df.loc[df.sample(frac=0.1).index, 'Product ID'] = 'InvalidID'

    # 8. Add unrealistic air temperature values
    df.loc[df.sample(frac=0.05).index, 'Air temperature [K]'] = 9999

    # 9. Add null values in required columns
    required_columns = ['Product ID', 'Type', 'Air temperature [K]', 'Process temperature [K]', 'Rotational speed [rpm]']
    for col in required_columns:
        df.loc[df.sample(frac=0.05).index, col] = None

    # 10. Set "Type" to an invalid category
    df.loc[df.sample(frac=0.1).index, 'Type'] = 'InvalidType'

    # 11. Replace "Tool wear [min]" with extreme outliers
    df.loc[df.sample(frac=0.1).index, 'Tool wear [min]'] = 99999

    # 12. Add invalid regex patterns to "Product ID"
    df.loc[df.sample(frac=0.1).index, 'Product ID'] = 'XYZ123'

    # 13. Flip air temperature and process temperature columns
    flipped_indices = df.sample(frac=0.1).index
    df.loc[flipped_indices, ['Air temperature [K]', 'Process temperature [K]']] = df.loc[
        flipped_indices, ['Process temperature [K]', 'Air temperature [K]']
    ].values

    # 14. Add null values only to "Torque [Nm]" column
    df.loc[df.sample(frac=0.1).index, 'Torque [Nm]'] = None

    # 15. Replace "Rotational speed [rpm]" with non-numeric strings
    df.loc[df.sample(frac=0.1).index, 'Rotational speed [rpm]'] = 'speed_error'

    # 16. Add random noise to "Air temperature [K]"
    # Convert column back to numeric, coercing errors to NaN
    df['Air temperature [K]'] = pd.to_numeric(df['Air temperature [K]'], errors='coerce')
    noise_indices = df.sample(frac=0.1).index
    df.loc[noise_indices, 'Air temperature [K]'] += np.random.normal(50, 20, size=len(noise_indices))

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

