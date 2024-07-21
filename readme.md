# Data Comparison Tool
This Python script compares two data files (CSV, Parquet, or SAS7BDAT) and computes checksums for common columns. It identifies matching and non-matching columns based on their checksums.

[![Test checksum_all_columns](https://github.com/uaineteine/python_data_checksum/actions/workflows/test.yaml/badge.svg)](https://github.com/uaineteine/python_data_checksum/actions/workflows/test.yaml)

## Usage
Clone this repository:
git clone https://github.com/your-username/data-comparison-tool.git
cd data-comparison-tool

## Install the required dependencies:
pip install pandas

## Run the script:
Open your terminal or command prompt and navigate to the directory where you saved script.py. Execute the script using the following command, replacing <file1>, <file2>, and <output_file> with the actual paths to your data files and the desired output filename:

```
    python script.py <file1> <file2> <output_file>
```
## Example:
```
    python execute.py data/file1.csv data/file2.parquet comparison_results.csv
```
This will compare data/file1.csv and data/file2.parquet, and save the results (matching and non-matching columns with checksums) to comparison_results.csv.

Note: Ensure the checksum_all_columns.py file (containing the process_dataframes function) is in the same directory as execute.py or adjust the import path accordingly.

View the results in the console. The script will display matching and non-matching columns along with their checksums.
#### Example Output
| **variable** | **checksum**                     | **status** |
|--------------|----------------------------------|------------|
| column1      | cb08ca4a7bb5f9683c19133a84872ca7 | match      |
| column3      | 9d4bbe2de1c65809c3dee92cdb8f9216 | nonmatch   |
| column2      | 81dc9bdb52d04dc20036dbd8313ed055 | nonmatch   |

## Contributing
Feel free to contribute by opening an issue or submitting a pull request. Your feedback and improvements are welcome!
