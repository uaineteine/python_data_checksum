import pandas as pd
import hashlib

def checksum_all_columns(file1, file2, file_type1='csv', file_type2='csv'):
    # Load the first file
    if file_type1 == 'parquet':
        df1 = pd.read_parquet(file1)
    elif file_type1 == 'sas7bdat':
        df1 = pd.read_sas(file1, format='sas7bdat')
    else:
        df1 = pd.read_csv(file1)
    
    # Load the second file
    if file_type2 == 'parquet':
        df2 = pd.read_parquet(file2)
    elif file_type2 == 'sas7bdat':
        df2 = pd.read_sas(file2, format='sas7bdat')
    else:
        df2 = pd.read_csv(file2)
    
    # Find common columns
    common_columns = set(df1.columns).intersection(set(df2.columns))
    missing_in_df1 = set(df2.columns) - set(df1.columns)
    missing_in_df2 = set(df1.columns) - set(df2.columns)
    
    # Compute MD5 checksums for the common columns and find matches
    matches = []
    for column in common_columns:
        df1['checksum'] = df1[column].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
        df2['checksum'] = df2[column].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
        match = df1[df1['checksum'].isin(df2['checksum'])][[column, 'checksum']]
        match['column'] = column
        matches.append(match)
    
    # Concatenate all matches into a single DataFrame
    matches_df = pd.concat(matches, ignore_index=True)
    
    # Create DataFrames for missing columns
    missing_in_df1_df = pd.DataFrame(list(missing_in_df1), columns=['missing_in_df1'])
    missing_in_df2_df = pd.DataFrame(list(missing_in_df2), columns=['missing_in_df2'])
    
    return matches_df, missing_in_df1_df, missing_in_df2_df

# Example usage
# matches_df, missing_in_df1_df, missing_in_df2_df = checksum_all_columns('file1.csv', 'file2.csv', 'csv', 'csv')
# print("Matches DataFrame:\n", matches_df)
# print("Missing in file1 DataFrame:\n", missing_in_df1_df)
# print("Missing in file2 DataFrame:\n", missing_in_df2_df)
