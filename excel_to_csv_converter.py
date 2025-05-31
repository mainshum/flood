#!/usr/bin/env python3

import os
import pandas as pd
import argparse
from pathlib import Path

def convert_excel_to_csv(input_folder: str, output_folder: str):
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Walk through all files in input folder
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.xlsx'):
                # Get full path of Excel file
                excel_path = os.path.join(root, file)
                
                # Read all sheets from Excel file
                excel_file = pd.ExcelFile(excel_path)
                
                # Convert each sheet to CSV
                for sheet_name in excel_file.sheet_names:
                    # Read the sheet
                    df = pd.read_excel(excel_path, sheet_name=sheet_name)
                    
                    # Create CSV filename
                    csv_filename = f"{Path(file).stem}_{sheet_name}.csv"
                    csv_path = os.path.join(output_folder, csv_filename)
                    
                    # Save as CSV
                    df.to_csv(csv_path, index=False)

def main():
    parser = argparse.ArgumentParser(description='Convert Excel files to CSV format')
    parser.add_argument('input_folder', help='Input folder containing Excel files')
    parser.add_argument('output_folder', help='Output folder for CSV files')
    
    args = parser.parse_args()
    
    convert_excel_to_csv(args.input_folder, args.output_folder)

if __name__ == '__main__':
    main() 