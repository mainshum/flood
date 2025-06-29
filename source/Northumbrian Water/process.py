from pathlib import Path
import sys
import os

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import logging
from flood_processors.base_processor import BaseFloodProcessor

class NorthumbrianWaterProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Northumbrian Water')
        self.data_dir = Path(__file__).parent

    def process_sewer_flooding_2010_2023(self, file_path: Path):
        """Process the main sewer flooding incident data file."""
        df = pd.read_excel(file_path, sheet_name='Sheet1')
        
        for _, row in df.iterrows():
            # Skip rows that are notes or headers
            if pd.isna(row['DATE']) or str(row['DATE']).startswith('*'):
                continue
                
            incident_date = self.standardize_date(row['DATE'])
            incident_type = f"{row['LOCATION']} - {row['Cause']}" if pd.notna(row['LOCATION']) and pd.notna(row['Cause']) else None
            
            location = self.create_location_dict(
                postcode=row['Postcode'] if pd.notna(row['Postcode']) else None
            )
            
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process(self):
        # Process the main granular data file
        main_file = self.data_dir / 'EIR22807 Sewer flooding incident data 2010 to 2023.xlsx'
        if main_file.exists():
            logging.info('Processing EIR22807 Sewer flooding incident data 2010 to 2023.xlsx')
            self.process_sewer_flooding_2010_2023(main_file)
        
        # Log that other files are skipped as they contain aggregated data
        logging.info('Skipping aggregated summary files:')
        logging.info('- EIR22807 Clean water flooding, incidents by area.xlsx (aggregated by area)')
        logging.info('- EIR22727 Sewer flooding incident data 2021 2022 2023.xlsx (aggregated summary)')
        logging.info('- EIR22727 Clean water flooding incident data 2021 2022 2023.xlsx (aggregated by area)')
        
        self.save_results()

if __name__ == "__main__":
    processor = NorthumbrianWaterProcessor()
    processor.process() 