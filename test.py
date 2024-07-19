import checksum_all_columns as ck
import pandas as pd

# Define your file paths and types
file1 = 'path/to/your/file1.csv'
file2 = 'path/to/your/file2.csv'
file_type1 = 'csv'
file_type2 = 'csv'

# Call the function
final_df = ck.process_dataframes(file1, file2, file_type1, file_type2)

# Save the output to a CSV file
output_file = 'output.csv'
final_df.to_csv(output_file, index=False)

print(f"Output saved to {output_file}")
