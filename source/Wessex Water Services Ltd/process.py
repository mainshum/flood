from pathlib import Path
import sys
import os

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import logging
from flood_processors.base_processor import BaseFloodProcessor

class WessexWaterProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Wessex Water Services Ltd')
        self.data_dir = Path(__file__).parent

    def process_eir2025_046(self, file_path: Path):
        df = pd.read_excel(file_path, sheet_name='Sewer Water Incident Data')
        for _, row in df.iterrows():
            incident_date = self.standardize_date(row['Date Reported'])
            incident_type = f"{row['Job Type']} - {row['High Level Fault']}" if pd.notna(row['Job Type']) and pd.notna(row['High Level Fault']) else None
            location = self.create_location_dict(
                postcode=row['Postcode'] if pd.notna(row['Postcode']) else None
            )
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process_eir2024_079(self, file_path: Path):
        df = pd.read_excel(file_path, sheet_name='Sewer flooding incident data 23')
        for _, row in df.iterrows():
            incident_date = self.standardize_date(row['Date Reported'])
            incident_type = f"{row['Job Type']} - {row['High Level Fault']}" if pd.notna(row['Job Type']) and pd.notna(row['High Level Fault']) else None
            location = self.create_location_dict(
                postcode=row['Postcode'] if pd.notna(row['Postcode']) else None
            )
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process_21_23_data(self, file_path: Path):
        df = pd.read_excel(file_path, sheet_name='Sewer Water Incident Data')
        for _, row in df.iterrows():
            incident_date = self.standardize_date(row['Date Reported'])
            incident_type = f"{row['Job Type']} - {row['High Level Fault']}" if pd.notna(row['Job Type']) and pd.notna(row['High Level Fault']) else None
            location = self.create_location_dict(
                postcode=row['Postcode'] if pd.notna(row['Postcode']) else None
            )
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process(self):
        # EIR2025-046
        file_2025_046 = self.data_dir / 'Flooding incidents EIR2025-046.xlsx'
        if file_2025_046.exists():
            logging.info('Processing Flooding incidents EIR2025-046.xlsx')
            self.process_eir2025_046(file_2025_046)
        
        # EIR2024-079
        file_2024_079 = self.data_dir / 'Sewer Flooding Incident Data 2023 EIR2024 079.xlsx'
        if file_2024_079.exists():
            logging.info('Processing Sewer Flooding Incident Data 2023 EIR2024 079.xlsx')
            self.process_eir2024_079(file_2024_079)
        
        # 21-23 data
        file_21_23 = self.data_dir / '21-23 data' / '2021 2023 Flooding incidents EIR2024 131.xlsx'
        if file_21_23.exists():
            logging.info('Processing 2021 2023 Flooding incidents EIR2024 131.xlsx')
            self.process_21_23_data(file_21_23)
        
        self.save_results()

if __name__ == "__main__":
    processor = WessexWaterProcessor()
    processor.process() 