import os
import pandas as pd
import numpy as np

def assign_error(df, column, value, frac=0.1):
    # Drop duplicates to avoid potential issues with duplicate indices
    df = df.drop_duplicates(subset=None, keep='first').reset_index(drop=True)
    
    # Sample a fraction of rows to introduce errors
    sampled_indices = df.sample(frac=frac).index
    
    # Assign the error value to the selected rows
    for idx in sampled_indices:
        df.at[idx, column] = value
    return df

def introduce_data_issues(df, severity_level="high", max_errors_percentage=0.2):
    # Cast columns to 'object' dtype before introducing incompatible data types
    df['Rotational speed [rpm]'] = df['Rotational speed [rpm]'].astype('object')
    df['Air temperature [K]'] = df['Air temperature [K]'].astype('object')
    df['Process temperature [K]'] = df['Process temperature [K]'].astype('object')
    df['Tool wear [min]'] = df['Tool wear [min]'].astype('object')  # Cast Tool wear to object dtype

    # Define all possible error functions in a dictionary
    errors = {
        1: lambda df: assign_error(df, 'Tool wear [min]', 'error'),  # High Severity
        2: lambda df: assign_error(df, 'Process temperature [K]', 'France'),  # High Severity
        3: lambda df: assign_error(df, 'Product ID', 'InvalidID'),  # High Severity
        4: lambda df: assign_error(df, 'Rotational speed [rpm]', 'speed_error'),  # High Severity
        5: lambda df: assign_error(df, 'Air temperature [K]', np.nan),  # Medium Severity
        6: lambda df: assign_error(df, 'Rotational speed [rpm]', -5000),  # Medium Severity
        7: lambda df: assign_error(df, 'Torque [Nm]', 1000),  # Medium Severity
        8: lambda df: assign_error(df, 'Air temperature [K]', 9999),  # Medium Severity
        9: lambda df: assign_error(df, 'Type', 'InvalidType'),  # Medium Severity
        10: lambda df: assign_error(df, 'Tool wear [min]', 99999),  # Medium Severity
        11: lambda df: assign_error(df, 'Product ID', 'XYZ123'),  # Medium Severity
        12: lambda df: assign_error(df, 'Air temperature [K]', pd.to_numeric(df['Air temperature [K]'], errors='coerce')),  # Medium Severity
        13: lambda df: assign_error(df, 'Air temperature [K]', df['Process temperature [K]']),  # Medium Severity
        14: lambda df: assign_error(df, 'Torque [Nm]', None),  # Low Severity
        15: lambda df: pd.concat([df, df.sample(frac=0.05)]),  # Low Severity (add duplicate rows)
        16: lambda df: assign_error(df, 'Product ID', None),  # Low Severity
    }

    # Choose severity level
    if severity_level == "high":
        # Apply between 12 to 16 high severity errors (randomly)
        num_errors = np.random.randint(12, 17)
        selected_errors = np.random.choice(range(1, 17), num_errors, replace=False)
        for err_num in selected_errors:
            df = errors[err_num](df)

    elif severity_level == "medium":
        # Apply between 6 to 12 medium severity errors (randomly)
        num_errors = np.random.randint(6, 13)
        selected_errors = np.random.choice(range(1, 17), num_errors, replace=False)
        for err_num in selected_errors:
            df = errors[err_num](df)

    elif severity_level == "low":
        # Apply between 1 to 4 low severity errors (randomly)
        num_errors = np.random.randint(1, 5)
        selected_errors = np.random.choice(range(1, 17), num_errors, replace=False)
        for err_num in selected_errors:
            df = errors[err_num](df)

    # Apply a random number of errors within the max_error_percentage limit
    max_errors = int(len(df) * max_errors_percentage)
    error_indices = np.random.choice(df.index, size=max_errors, replace=False)
    df.loc[error_indices, 'ErrorFlag'] = 'Error'  # Flag errors in a new column
    
    return df


def split_and_save_data(df, output_folder, num_files, max_errors_percentage):
    os.makedirs(output_folder, exist_ok=True)
    chunk_size = len(df) // num_files

    for i in range(num_files):
        start_index = i * chunk_size
        end_index = start_index + chunk_size if i < num_files - 1 else None
        split_df = df.iloc[start_index:end_index]
        
        # Randomly select severity level ("high", "medium", "low")
        severity_level = np.random.choice(["high", "medium", "low"])
        
        # Introduce errors to the split data
        split_df_with_errors = introduce_data_issues(split_df, severity_level, max_errors_percentage)
        
        # Save the split DataFrame to CSV
        split_df_with_errors.to_csv(os.path.join(output_folder, f'data_split_{i}.csv'), index=False)
        

if __name__ == "__main__":
    main_dataset_path = 'data/onlyfeatures.csv'
    main_df = pd.read_csv(main_dataset_path)

    
    output_folder = 'raw-data'
    num_files_to_generate = 25
    split_and_save_data(main_df, output_folder, num_files_to_generate, max_errors_percentage=0.2)
