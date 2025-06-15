from pathlib import Path
import sys
import logging

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
from flood_processors.base_processor import BaseFloodProcessor

class AnglianWaterProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Anglian Water')
        self.data_dir = Path(__file__).parent

    def process_2010_2020_data(self, file_path: Path):
        """Process Anglian Water's 2010-2020 data file."""
        # Read the data sheet
        df = pd.read_excel(file_path, sheet_name='Data Request')
        
        # Read the legend sheet for decoding
        legend_df = pd.read_excel(file_path, sheet_name='Legend')
        
        # Extract cause code mappings from legend
        cause_codes = {}
        for idx, row in legend_df.iterrows():
            if pd.notna(row['Unnamed: 0']) and ' - ' in str(row['Unnamed: 0']):
                code, desc = row['Unnamed: 0'].split(' - ')
                cause_codes[code.strip()] = desc.strip()
        for _, row in df.iterrows():
            # Standardize date
            incident_date = self.standardize_date(row['Incident date'])
            
            # Get incident type information
            cause_code = row['Cause code']
            flooding_type = row['Flooding type']
            flooding_sub_type = row['Flooding sub type']
            
            # Map codes to descriptions
            cause_desc = cause_codes.get(cause_code, 'Unknown')
            
            # Add record
            self.add_record(
                incident_date=incident_date,
                incident_type=cause_desc,
                location = { 'type': 'town', 'value': row['City']},
            )

    def process_2023_data(self, file_path: Path):
        """Process Anglian Water's 2023 data file."""
        # Read the data sheet
        df = pd.read_excel(file_path, sheet_name='Sheet1')
        
        for _, row in df.iterrows():
            # Standardize date
            incident_date = self.standardize_date(row['Incident date'])
            
            # Add record
            self.add_record(
                incident_date=incident_date,
                location = { 'type': 'postcode', 'value': row['First Half Post Code']},
            )

    def process_2nd_request_data(self, file_path: Path):
        """Process Anglian Water's second request data file."""
        # Read the data sheet
        df = pd.read_excel(file_path, sheet_name='Data')
        
        # Read the legend sheet for decoding
        legend_df = pd.read_excel(file_path, sheet_name='Legend')
        
        # Extract cause code mappings from legend
        cause_codes = {}
        for _, row in legend_df.iterrows():
            if pd.notna(row['Unnamed: 0']) and ' - ' in str(row['Unnamed: 0']):
                code, desc = row['Unnamed: 0'].split(' - ')
                cause_codes[code.strip()] = desc.strip()
        
        for _, row in df.iterrows():
            # Standardize date
            incident_date = self.standardize_date(row['Incident date'])
            
            # Get incident type information
            cause_code = row['Cause code']
            
            # Map codes to descriptions
            cause_desc = cause_codes.get(cause_code, 'Unknown')
            
            # Add record
            self.add_record(
                incident_date=incident_date,
                incident_type=cause_desc,
                location = { 'type': 'town', 'value': row['City']},
            )

    def process(self):
        """Main processing method."""
        # Process each file
        for excel_file in self.data_dir.glob('*.xlsx'):
            if excel_file.name.startswith('.'):  # Skip hidden files
                continue
                
            if excel_file.name == 'Flooding data 2010 to 2020.xlsx':
                self.process_2010_2020_data(excel_file)
            elif excel_file.name == '2023 data.xlsx':
                #self.process_2023_data(excel_file)
                pass
            elif excel_file.name == '2nd request data (1).xlsx':
                self.process_2nd_request_data(excel_file)
            else:
                logging.warning(f"Skipping unknown file: {excel_file.name}")
        
        # Save results
        self.save_results()

if __name__ == "__main__":
    processor = AnglianWaterProcessor()
    processor.process() 