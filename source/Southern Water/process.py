from pathlib import Path
import sys
import os

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import logging
from flood_processors.base_processor import BaseFloodProcessor

class SouthernWaterProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Southern Water')
        self.data_dir = Path(__file__).parent

    def process_southern_water_2023(self, file_path: Path):
        """Process Southern Water's 2023 sewer incidents data."""
        # Process main data sheet
        df = pd.read_excel(file_path, sheet_name='Sewer Incidents 2023')
        
        for _, row in df.iterrows():
            # Standardize date (format: YYYYMMDD)
            incident_date_str = str(row['Incident_Date'])
            if len(incident_date_str) == 8:
                incident_date = f"{incident_date_str[:4]}-{incident_date_str[4:6]}-{incident_date_str[6:8]}"
            else:
                incident_date = self.standardize_date(row['Incident_Date'])
            
            # Create incident type string
            incident_type = row['Cause'] if pd.notna(row['Cause']) else 'Unknown'
            
            # Create location dictionary
            location = self.create_location_dict(
                postcode=row['Post Code Short'] if pd.notna(row['Post Code Short']) else None,
                town=row['posttown'] if pd.notna(row['posttown']) else None,
                county=row['county'] if pd.notna(row['county']) else None
            )
            
            # Add record
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process_southern_water_suspicious(self, file_path: Path):
        """Process Southern Water's suspicious incidents data."""
        # Process suspicious data sheet
        df = pd.read_excel(file_path, sheet_name='suspicious (louis)')
        
        for _, row in df.iterrows():
            # Standardize date (format: YYYYMMDD)
            incident_date_str = str(row['Incident_Date'])
            if len(incident_date_str) == 8:
                incident_date = f"{incident_date_str[:4]}-{incident_date_str[4:6]}-{incident_date_str[6:8]}"
            else:
                incident_date = self.standardize_date(row['Incident_Date'])
            
            # Create incident type string
            incident_type = row['Cause'] if pd.notna(row['Cause']) else 'Unknown'
            
            # Create location dictionary
            location = self.create_location_dict(
                postcode=row['Post Code Short'] if pd.notna(row['Post Code Short']) else None,
                town=row['posttown'] if pd.notna(row['posttown']) else None,
                county=row['county'] if pd.notna(row['county']) else None
            )
            
            # Add record
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process_southwest_water_2023(self, file_path: Path):
        """Process Southwest Water's 2023 data."""
        df = pd.read_excel(file_path, sheet_name='Data')
        
        for _, row in df.iterrows():
            # Standardize date
            incident_date = self.standardize_date(row['Date Raised'])
            
            # Create incident type string
            incident_type = row['Feedback Cause'] if pd.notna(row['Feedback Cause']) else 'Unknown'
            
            # Create location dictionary
            location = self.create_location_dict(
                postcode=row['Postcode'] if pd.notna(row['Postcode']) else None,
                town=row['Town/City'] if pd.notna(row['Town/City']) else None
            )
            
            # Add record
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process_southwest_water_historical(self, file_path: Path):
        """Process Southwest Water's historical data."""
        # Read the data sheet
        df = pd.read_excel(file_path, sheet_name='Data')
        
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
            
            # Create incident type string
            incident_type = f"{cause_desc} - Type {flooding_type} - Sub-type {flooding_sub_type}"
            
            # Create location dictionary
            location = self.create_location_dict(
                town=row['City'] if pd.notna(row['City']) else None,
                district=row['District'] if pd.notna(row['District']) else None
            )
            
            # Add record
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process(self):
        """Main processing method."""
        # Process Southern Water main data
        main_file = self.data_dir / '2023 Sewer Incidents.xlsx'
        if main_file.exists():
            logging.info("Processing Southern Water 2023 data")
            self.process_southern_water_2023(main_file)
            self.process_southern_water_suspicious(main_file)
        
        # Process Southwest Water data
        southwest_dir = self.data_dir / 'Southwest Water'
        if southwest_dir.exists():
            # Process 2023 data
            southwest_2023_file = southwest_dir / 'EIR24187.xlsx'
            if southwest_2023_file.exists():
                logging.info("Processing Southwest Water 2023 data")
                self.process_southwest_water_2023(southwest_2023_file)
            
            # Process historical data
            historical_file = southwest_dir / '2nd request' / '1405 Flooding data.xlsx'
            if historical_file.exists():
                logging.info("Processing Southwest Water historical data")
                self.process_southwest_water_historical(historical_file)
        
        # Save results
        self.save_results()

if __name__ == "__main__":
    processor = SouthernWaterProcessor()
    processor.process() 