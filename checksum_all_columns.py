import os
import pandas as pd
import hashlib

def load_file(file_path):
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.parquet':
        return pd.read_parquet(file_path)
    elif file_extension == '.sas7bdat':
        return pd.read_sas(file_path, format='sas7bdat')
    else:
        return pd.read_csv(file_path)

def checksum_all_columns(file1, file2):
    df1 = load_file(file1)
    df2 = load_file(file2)
    
    # Find common columns
    common_columns = set(df1.columns).intersection(set(df2.columns))
    
    # Compute checksums for the common columns
    matches = []
    non_matches = []
    for column in common_columns:
        checksum1 = hashlib.md5(df1[column].astype(str).sum().encode()).hexdigest()
        checksum2 = hashlib.md5(df2[column].astype(str).sum().encode()).hexdigest()
        if checksum1 == checksum2:
            matches.append({'column': column, 'checksum': checksum1})
        else:
            non_matches.append({'column': column, 'checksum': checksum1})
    return pd.DataFrame(matches), pd.DataFrame(non_matches)

def process_dataframes(file1, file2):
    # Call the checksum_all_columns function
    matches_df, non_matching_df = checksum_all_columns(file1, file2)

    # Add 'status' column
    matches_df['status'] = 'match'
    non_matching_df['status'] = 'nonmatch'
    
    # Rename columns to avoid conflicts
    matches_df = matches_df.rename(columns={'column': 'variable'})
    non_matching_df = non_matching_df.rename(columns={'column': 'variable'})
    
    # Append all DataFrames
    final_df = pd.concat([matches_df, non_matching_df], ignore_index=True)
    
    return final_df

# Example usage
# matches_df, missing_in_df1_df, missing_in_df2_df = checksum_all_columns('file1.csv', 'file2.csv', 'csv', 'csv')
# print("Matches DataFrame:\n", matches_df)
# print("Missing in file1 DataFrame:\n", missing_in_df1_df)
# print("Missing in file2 DataFrame:\n", missing_in_df2_df)
