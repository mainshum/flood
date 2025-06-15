import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('flood_data_extraction.log'),
        logging.StreamHandler()
    ]
)

# UK Postcode regex pattern
UK_POSTCODE_PATTERN = r'^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$'

class FloodDataExtractor:
    def __init__(self, source_dir: str):
        self.source_dir = Path(source_dir)
        self.companies = [d for d in self.source_dir.iterdir() if d.is_dir()]
        self.standardized_data = []
        
    def standardize_date(self, date_value) -> Tuple[str, str]:
        """
        Standardize date to YYYY-MM-DD format.
        Returns tuple of (standardized_date, precision)
        """
        if pd.isna(date_value):
            return None, None
            
        try:
            if isinstance(date_value, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                    try:
                        date_obj = datetime.strptime(date_value, fmt)
                        return date_obj.strftime('%Y-%m-%d'), 'full_date'
                    except ValueError:
                        continue
                
                # Try year only
                if re.match(r'^\d{4}$', date_value):
                    return f"{date_value}-01-01", 'year_only'
                    
                # Try year and month
                if re.match(r'^\d{4}-\d{2}$', date_value):
                    return f"{date_value}-01", 'year_month'
                    
            elif isinstance(date_value, (pd.Timestamp, datetime)):
                return date_value.strftime('%Y-%m-%d'), 'full_date'
                
        except Exception as e:
            logging.warning(f"Date standardization failed for value {date_value}: {str(e)}")
            return None, None
            
        return None, None

    def process_anglian_water_2010_2020(self, file_path: Path) -> List[Dict]:
        """Process Anglian Water's 2010-2020 data file."""
        records = []
        
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
            incident_date, date_precision = self.standardize_date(row['Incident date'])
            
            # Get incident type information
            cause_code = row['Cause code']
            flooding_type = row['Flooding type']
            flooding_sub_type = row['Flooding sub type']
            
            # Map codes to descriptions
            cause_desc = cause_codes.get(cause_code, 'Unknown')
            
            # Combine location information
            location = {
                'postcode': None,  # No postcode in this dataset
                'town': row['City'] if pd.notna(row['City']) else None,
                'district': row['District'] if pd.notna(row['District']) else None
            }
            
            record = {
                'company': 'Anglian Water',
                'incident_date': incident_date,
                'date_precision': date_precision,
                'incident_type': {
                    'cause': cause_desc,
                    'flooding_type': f"Type {flooding_type}",
                    'flooding_sub_type': f"Sub-type {flooding_sub_type}"
                },
                'location': location,
                'sewer_type': row['Sewer type'] if pd.notna(row['Sewer type']) else None,
                'weather': row['Weather'] if pd.notna(row['Weather']) else None,
                'spill_size': row['Spill Size'] if pd.notna(row['Spill Size']) else None
            }
            
            records.append(record)
            
        return records

    def process_anglian_water_2023(self, file_path: Path) -> List[Dict]:
        """Process Anglian Water's 2023 data file."""
        records = []
        
        # Read the data sheet
        df = pd.read_excel(file_path, sheet_name='Sheet1')
        
        for _, row in df.iterrows():
            # Standardize date
            incident_date, date_precision = self.standardize_date(row['Incident date'])
            
            # Combine location information
            location = {
                'postcode': row['First Half Post Code'] if pd.notna(row['First Half Post Code']) else None,
                'town': row['City'] if pd.notna(row['City']) else None,
                'district': row['District'] if pd.notna(row['District']) else None
            }
            
            record = {
                'company': 'Anglian Water',
                'incident_date': incident_date,
                'date_precision': date_precision,
                'incident_type': {
                    'category': row['Category'] if pd.notna(row['Category']) else None,
                    'flooding_sub_type': row['Flooding Sub Type'] if pd.notna(row['Flooding Sub Type']) else None
                },
                'location': location,
                'sewer_status': row['Sewer Status'] if pd.notna(row['Sewer Status']) else None,
                'action_code': row['Action Code'] if pd.notna(row['Action Code']) else None
            }
            
            records.append(record)
            
        return records

    def process_anglian_water_2nd_request(self, file_path: Path) -> List[Dict]:
        """Process Anglian Water's second request data file."""
        records = []
        
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
            incident_date, date_precision = self.standardize_date(row['Incident date'])
            
            # Get incident type information
            cause_code = row['Cause code']
            flooding_type = row['Flooding type']
            flooding_sub_type = row['Flooding sub type']
            
            # Map codes to descriptions
            cause_desc = cause_codes.get(cause_code, 'Unknown')
            
            # Combine location information
            location = {
                'postcode': None,  # No postcode in this dataset
                'town': row['City'] if pd.notna(row['City']) else None,
                'district': row['District'] if pd.notna(row['District']) else None
            }
            
            record = {
                'company': 'Anglian Water',
                'incident_date': incident_date,
                'date_precision': date_precision,
                'incident_type': {
                    'cause': cause_desc,
                    'flooding_type': f"Type {flooding_type}",
                    'flooding_sub_type': f"Sub-type {flooding_sub_type}"
                },
                'location': location,
                'sewer_type': row['Sewer type'] if pd.notna(row['Sewer type']) else None,
                'weather': row['Weather'] if pd.notna(row['Weather']) else None,
                'spill_size': row['Size spill'] if pd.notna(row['Size spill']) else None
            }
            
            records.append(record)
            
        return records

    def process_company_data(self, company_dir: Path) -> List[Dict]:
        """
        Process all Excel files in a company directory.
        Returns list of standardized records.
        """
        company_name = company_dir.name
        logging.info(f"Processing data for {company_name}")
        
        records = []
        
        if company_name == 'Anglian Water':
            # Process each file with its specific method
            for excel_file in company_dir.glob('*.xlsx'):
                if excel_file.name.startswith('.'):  # Skip hidden files
                    continue
                    
                logging.info(f"Processing file: {excel_file.name}")
                
                if excel_file.name == 'Flooding data 2010 to 2020.xlsx':
                    records.extend(self.process_anglian_water_2010_2020(excel_file))
                elif excel_file.name == '2023 data.xlsx':
                    records.extend(self.process_anglian_water_2023(excel_file))
                elif excel_file.name == '2nd request data (1).xlsx':
                    records.extend(self.process_anglian_water_2nd_request(excel_file))
                else:
                    logging.warning(f"Skipping unknown file: {excel_file.name}")
        else:
            logging.warning(f"No processing logic implemented for {company_name}")
                
        return records

    def save_results(self, output_path: str):
        """Save standardized data to parquet file."""
        if not self.standardized_data:
            logging.warning("No data to save!")
            return
            
        df = pd.DataFrame(self.standardized_data)
        df.to_parquet(output_path, index=False)
        logging.info(f"Saved {len(df)} records to {output_path}")

def main():
    extractor = FloodDataExtractor('source')
    
    # Process each company's data
    for company_dir in extractor.companies:
        records = extractor.process_company_data(company_dir)
        extractor.standardized_data.extend(records)
    
    # Save results
    extractor.save_results('flood_incidents.parquet')

if __name__ == "__main__":
    main() 