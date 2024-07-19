     matches_df, missing_in_df1_df, missing_in_df2_df = checksum_all_columns(file1, file2, file_type1, file_type2)
    
    # Add 'status' column
    matches_df['status'] = 'match'
    missing_in_df1_df['status'] = 'missing_in_df1'
    missing_in_df2_df['status'] = 'missing_in_df2'
    
    # Rename columns to avoid conflicts
    matches_df = matches_df.rename(columns={'column': 'variable'})
    missing_in_df1_df = missing_in_df1_df.rename(columns={'missing_in_df1': 'variable'})
    missing_in_df2_df = missing_in_df2_df.rename(columns={'missing_in_df2': 'variable'})
    
    # Append all DataFrames
    final_df = pd.concat([matches_df, missing_in_df1_df, missing_in_df2_df], ignore_index=True)
